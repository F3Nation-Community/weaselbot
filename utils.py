"""General purpose utilites for Weaselbot. Broadly speaking, if there's a effort to merge all
these different tools together, this module would be a landing spot for all those shared
methods.
"""

import logging
import os
import ssl
import time
from collections import Counter, defaultdict
from typing import NamedTuple

import pandas as pd
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def mysql_connection() -> Engine:
    """
    Connect to MySQL. This involves loading environment variables from file
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv()
    engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:3306"
    )
    return engine


def slack_client(token: str) -> WebClient:
    """
    Instantiate Slack Web client

    :param token: Slack private token for the given channel
    :param type: str
    :return: open webclient to the slack channel
    :rtype: slack_sdk.WebClient object
    """

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return WebClient(token=token, ssl=ssl_context)


def _check_for_new_results(
    row: NamedTuple, year: int, idx: int, df: pd.DataFrame, awarded: pd.DataFrame
) -> pd.DataFrame:
    """
    Check for new earned achievements in the data. By looking at the current
    achievement number and comparing it against what we've already seen, determine
    if there are new achievments to issue. If there are no new achievments,
    continue to the next one.

    :param row: a key: value named tuple.
    :type row: namedtuple produced by pandas' intertuples method
    :param year: the 4-digit current year
    :type year: int
    :param idx: The index of the `awards table` award we are focusing on
    :type idx: int
    :param df: the region data set. This includes new records, if any.
    :type df: pd.DataFrame
    :param awarded: Table of awards already handed out to the pax in the region
    :type awarded: pd.DataFrame
    :return: pandas DataFrame
    :rtype: pd.DataFrame object
    """

    match df.columns[0]:
        case "month":
            return (
                (
                    df.rename({"slack_user_id": "pax_id"}, axis=1)
                    .query("home_region == @row.paxminer_schema")
                    .merge(
                        awarded.assign(month=awarded.date_awarded.dt.month).query(
                            "achievement_id == @idx and date_awarded.dt.year == @year"
                        )[["month", "pax_id"]],
                        on=["month", "pax_id"],
                        how="left",
                        indicator=True,
                    )
                )
                .loc[lambda x: x._merge == "left_only"]
                .drop("_merge", axis=1)
            )
        case "week":
            return (
                (
                    df.rename({"slack_user_id": "pax_id"}, axis=1)
                    .query("home_region == @row.paxminer_schema")
                    .merge(
                        awarded.assign(week=awarded.date_awarded.dt.isocalendar().week).query(
                            "achievement_id == @idx and date_awarded.dt.year == @year"
                        )[["week", "pax_id"]],
                        on=["week", "pax_id"],
                        how="left",
                        indicator=True,
                    )
                )
                .loc[lambda x: x._merge == "left_only"]
                .drop("_merge", axis=1)
            )
        case _:
            return (
                (
                    df.rename({"slack_user_id": "pax_id"}, axis=1)
                    .query("home_region == @row.paxminer_schema")
                    .merge(
                        awarded.assign(year=awarded.date_awarded.dt.year).query(
                            "achievement_id == @idx and date_awarded.dt.year == @year"
                        )[["year", "pax_id"]],
                        on=["year", "pax_id"],
                        how="left",
                        indicator=True,
                    )
                )
                .loc[lambda x: x._merge == "left_only"]
                .drop("_merge", axis=1)
            )


def ordinal_suffix(n: int) -> str:
    """
    Logic to add the orginal suffix to the numbers.
    i.e. 3rd, 9th, 1st, etc...
    """
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    return suffix


def send_to_slack(
    row: NamedTuple,
    year: int,
    awarded: pd.DataFrame,
    awards: pd.DataFrame,
    dfs: list[pd.DataFrame],
    paxminer_log_channel: str,
) -> pd.DataFrame:
    """
    Take the region data set and for new records, write them to the `achievements_awarded` table along with
    sending the notification to Slack.

    Take the data and, after comparing it to the already-awarded achievements, find what hasn't been
    awarded. This also makes comparisons to each regions `awards_list` table. Some regions have customized it
    to exclude some base awards and include some custom ones. Custom awards are not taken into account. They must
    be separately addressed.

    Loop over each award and grant as necessary. Then push a Slack notification to both the region Slack channel
    and the person running this script so that there's a record of the run.

    :param row: a key: value named tuple.
    :type row: namedtuple produced by pandas' intertuples method
    :param year: the 4-digit current year
    :type year: int
    :param awarded: pandas dataframe of previously awarded achievements
    :type awarded: pd.DataFrame
    :param awards: dataframe with all achievable awards
    :type awards: pd.DataFrame
    :param dfs: collection of all regional data. Each dataframe is the data for one specific award
    :type dfs: list of pd.DataFrame objects
    :return: The final set of data that reflects news awarded achievements and needs to be appended to the region
    `awarded` table.
    :rtype: pd.DataFrame
    """

    client = slack_client(row.slack_token)  # only need one client per row (region)
    data_to_upload = pd.DataFrame()

    # Instantiate a counter for each pax. This is how we'll track total award earned counts between
    # what they already have (historcial) and what they're earning right now.
    _d = defaultdict(Counter)
    for r in (
        awarded.query("date_awarded.dt.year == @year")
        .groupby(["pax_id", "achievement_id"])["id"]
        .count()
        .reset_index(level=1)
        .itertuples()
    ):
        _d[r.Index].update({r.achievement_id: r.id})

    for idx, df in enumerate(dfs, start=1):
        if df.empty:
            # no one anywhere got this award. No sense wasting resources on it.
            try:
                logging.info(f"No data in {awards.loc[awards.id == idx, 'name'].values[0]} for {row.paxminer_schema}")
            except IndexError:
                logging.error(f"{row.paxminer_schema} doesn't have achievement {idx} in their awards_list table.")
            continue
        new_data = _check_for_new_results(row, year, idx, df, awarded)
        if new_data.empty:
            # there is data but nothing new since the last run. Carry on.
            try:
                logging.info(
                    f"{row.paxminer_schema} has data but nothing new for {awards.loc[awards.id == idx, 'name'].values[0]}."
                )
            except IndexError:
                logging.error(f"{row.paxminer_schema} has new data but doesn't track achievement_id {idx}.")
            continue

        # we got this far so there are achievements to award.
        if idx not in awards.id.tolist():
            logging.error(f"{row.paxminer_schema} doesn't track achievement_id {idx}.")
            continue

        # Loop over each record in `new_data`, assiging as appropriate
        for record in new_data.itertuples(index=False):
            _d[record.pax_id].update({idx: 1})
            new_award_name = awards.query("id == @idx")["name"].item()
            new_award_verb = awards.query("id == @idx")["verb"].item()
            total_achievements = _d[record.pax_id].total()
            total_idx_achievements = _d[record.pax_id][idx]
            ending = ordinal_suffix(total_idx_achievements)

            sMessage = [
                f"Congrats to our man <@{record.pax_id}>! ",
                f"He just unlocked the achievement *{new_award_name}* for {new_award_verb}. ",
                f"This is achievement #{total_achievements} for <@{record.pax_id}> and the {total_idx_achievements}{ending} ",
                "time this year he's earned this award. Keep up the good work!",
            ]
            sMessage = "".join(sMessage)
            logging.info(sMessage)
            try:
                response = client.chat_postMessage(channel=row.achievement_channel, text=sMessage, link_names=True)
                client.reactions_add(channel=row.achievement_channel, name="fire", timestamp=response.get("ts"))
                logging.info("Successfully added reaction.")
                logging.info(f"Successfully sent slack message for {record.pax_id} and achievement {idx}")
            except SlackApiError as e:
                if e.response.status_code == 429:
                    delay = int(e.response.headers["Retry-After"])
                    logging.info(f"Pausing Slack notifications for {delay} seconds.")
                    time.sleep(delay)
                    response = client.chat_postMessage(channel=row.achievement_channel, text=sMessage, link_names=True)
                    client.reactions_add(channel=row.achievement_channel, name="fire", timestamp=response.get("ts"))
                    logging.info("Successfully added reaction.")
                    logging.info(f"Successfully sent slack message for {record.pax_id} and achievement {idx}")
                else:
                    logging.error(f"Received the following error when posting for region {row.paxminer_schema} for achievement {new_award_name}")
                    logging.error(e)
                    # logging.error(f"Slack API gave error code: {e.response.status_code}")
                    continue

        new_data["achievement_id"] = idx
        data_to_upload = pd.concat(
            [data_to_upload, new_data[["achievement_id", "pax_id", "date_awarded"]]], ignore_index=True
        )

    try:
        client.chat_postMessage(
            channel=paxminer_log_channel,
            text=f"Successfully ran today's Weaselbot achievements patch. Sent {data_to_upload.shape[0]} new achievements.",
        )
    except SlackApiError as e:
        logging.error(f"Error sending message to {paxminer_log_channel} for {row.paxminer_schema}: {e}")

    logging.info(f"Sent all slack messages to {row.paxminer_schema} for achievement {idx}")

    return data_to_upload
