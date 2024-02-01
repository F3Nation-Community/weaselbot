import logging
import os
import ssl
import time
from typing import NamedTuple

import pandas as pd
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logging.basicConfig(format="%(asctime)s [%(levelname)s]:%(message)s",
                        level=logging.INFO,
                        datefmt="%Y-%m-%d %H:%M:%S")


def mysql_connection() -> Engine:
    """Connect to MySQL. This involves loading environment variables from file"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv()
    engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:3306"
    )
    return engine


def slack_client(token: str) -> WebClient:
    """Instantiate Slack Web client"""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return WebClient(token=token, ssl=ssl_context)


def _check_for_new_results(row: NamedTuple, year: int, idx: int, df: pd.DataFrame, awarded: pd.DataFrame) -> bool:
    """Check for new earned achievements in the data. By looking at the current
    achievement number and comparing it against what we've already seen, determine
    if there are new achievments to issue. If there are no new achievments,
    continue to the next one."""

    match df.columns[0]:
        case "month":
            return (
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
            ).loc[lambda x: x._merge == 'left_only'].drop("_merge", axis=1)
        case "week":
            return (
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
            ).loc[lambda x: x._merge == 'left_only'].drop("_merge", axis=1)
        case _:
            return (
                df.rename({"slack_user_id": "pax_id"}, axis=1)
                .query("home_region == @row.paxminer_schema")
                .merge(
                    awarded.assign(year=awarded.date_awarded.dt.month).query(
                        "achievement_id == @idx and date_awarded.dt.year == @year"
                    )[["year", "pax_id"]],
                    on=["year", "pax_id"],
                    how="left",
                    indicator=True,
                )
            ).loc[lambda x: x._merge == 'left_only'].drop("_merge", axis=1)

def message_constructor(idx: int, record: NamedTuple):
    """Construct the Slack message. This will have options based on
    whether or not the award is annual, monthly or weekly."""


def ordinal_suffix(x):
    j, k = x % 10, x % 100
    if j == 1 and k != 11:
        return "st"
    if j == 2 and k != 22:
        return "nd"
    if j == 3 and k != 13:
        return "rd"
    return "th"

def send_to_slack(row: NamedTuple, year: int, awarded: pd.DataFrame, awards: pd.DataFrame, dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Take the region data set and for new records, write them to the `achievements_awarded` table along with
    sending the notification to Slack.

    Take the data and, after comparing it to the already-awarded achievements, find what hasn't been
    awarded. This also makes comparisons to each regions `awards_list` table. Some regions have customized it
    to exclude some base awards and include some custom ones. Custom awards are not taken into account. They must
    be separately addressed.

    Loop over each award and grant as necessary. Then push a Slack notification to both the region Slack channel
    and the person running this script so that there's a record of the run.
    """

    client = slack_client(row.slack_token) # only need one client per row (region)
    data_to_upload = pd.DataFrame()

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
                logging.info(f"{row.paxminer_schema} has data but nothing new for {awards.loc[awards.id == idx, 'name'].values[0]}.")
            except IndexError:
                logging.error(f"{row.paxminer_schema} has new data but doesn't track achievement_id {idx}.")
            continue

        # we got this far so there are achievements to award.
        if idx not in awards.id.tolist():
            logging.error(f"{row.paxminer_schema} doesn't track achievement_id {idx}.")
            continue

        # Loop over each record in `new_data`, assiging as appropriate
        for record in new_data.itertuples(index=False):
            pax = record.pax_id
            new_award_name = awards.query("id == @idx")["name"].item()
            new_award_verb = awards.query("id == @idx")["verb"].item()
            total_achievements = awarded.query("pax_id == @pax and date_awarded.dt.year == @year").shape[0] + 1
            total_idx_achievements = awarded.query("achievement_id == @idx and pax_id == @pax and date_awarded.dt.year == @year").shape[0] + 1
            ending = ordinal_suffix(total_idx_achievements)

            sMessage = [f"Congrats to our man <@{pax}>! ",
            f"He just unlocked the achievement *{new_award_name}* for {new_award_verb}. ",
            f"This is achievement #{total_achievements} for <@{pax}> and the {total_idx_achievements}{ending} ",
            "time this year he's earned this award. Keep up the good work!"]
            sMessage = "".join(sMessage)
            try:
                response = client.chat_postMessage(channel=row.achievement_channel, text=sMessage, link_names=True)
                logging.info(f"Successfully sent slack message for {pax} and achievement {idx}")
            except SlackApiError as e:
                if e.response.status_code == 429:
                    delay = int(e.response.headers['Retry-After'])
                    logging.info(f"Pausing Slack notifications for {delay} seconds.")
                    time.sleep(delay)
                    response = client.chat_postMessage(channel=row.achievement_channel, text=sMessage, link_names=True)
                    logging.info(f"Successfully sent slack message for {pax} and achievement {idx}")
            client.reactions_add(channel=row.achievement_channel, name="fire", timestamp=response["ts"])
            logging.info("Successfully added reaction.")

        logging.info(f"Successfully sent all slack messages to {row.paxminer_schema} for achievement {idx}")


        new_data["achievement_id"] = idx
        data_to_upload = pd.concat([data_to_upload, new_data[["achievement_id", "pax_id", "date_awarded"]]], ignore_index=True)
    return data_to_upload


