"""
This module provides utility functions for connecting to MySQL, interacting with Slack, and processing
achievement data.

Functions:
    mysql_connection() -> Engine:
        Connect to MySQL. This involves loading environment variables from file.

    slack_client(token: str) -> WebClient:
        Instantiate Slack Web client.

    _check_for_new_results(schema: str, year: int, idx: int, df: pl.DataFrame, awarded: pl.DataFrame) -> pl.DataFrame:
        Check for new earned achievements in the data. By looking at the current achievement number and comparing it
        against what we've already seen, determine if there are new achievements to issue. If there are no new
        achievements, continue to the next one.

    ordinal_suffix(n: int) -> str:
        Logic to add the ordinal suffix to the numbers. i.e. 3rd, 9th, 1st, etc...

    send_to_slack(

        Take the data and, after comparing it to the already-awarded achievements, find what hasn't been awarded.
        This also makes comparisons to each region's `awards_list` table. Some regions have customized it to exclude
        some base awards and include some custom ones. Custom awards are not taken into account. They must be
        separately addressed.
"""

import logging
import os
import ssl
import time
from collections import Counter, defaultdict

import polars as pl
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def mysql_connection() -> Engine:
    """
    Establishes a connection to a MySQL database using environment variables.
    The function changes the current working directory to the directory of the script,
    loads environment variables from a .env file, and creates a SQLAlchemy engine
    for connecting to a MySQL database using the mysqlconnector driver.
    Returns:
        Engine: A SQLAlchemy Engine instance connected to the MySQL database.
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


def _check_for_new_results(schema: str, year: int, idx: int, df: pl.DataFrame, awarded: pl.DataFrame) -> pl.DataFrame:
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
    :type df: pl.DataFrame
    :param awarded: Table of awards already handed out to the pax in the region
    :type awarded: pl.DataFrame
    :return: pandas DataFrame
    :rtype: pl.DataFrame object
    """

    match df.columns[0]:
        case "month":
            with_cols = pl.col("date_awarded").dt.month().alias("month")
            select_col = "month"
        case "week":
            with_cols = pl.col("date_awarded").dt.week().alias("week")
            select_col = "week"
        case _:
            with_cols = pl.col("date_awarded").dt.year().alias("year")
            select_col = "year"

    return (
        df.with_columns(pl.col("slack_user_id").alias("pax_id"))
        .drop("slack_user_id")
        .filter(pl.col("region") == schema)
        .join(
            awarded.with_columns(with_cols)
            .filter((pl.col("achievement_id").cast(pl.Int64()) == idx) & (pl.col("date_awarded").dt.year() == year))
            .select([select_col, "pax_id"]),
            on=[select_col, "pax_id"],
            how="anti",
            join_nulls=True,
        )
    )


def ordinal_suffix(n: int) -> str:
    """
    Logic to add the ordinal suffix to the numbers.
    i.e. 3rd, 9th, 1st, etc...
    """
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    return suffix


def _get_achievement_counts(awarded: pl.DataFrame, year: int) -> dict[str, Counter]:
    """Get achievement counts for each pax in the current year."""
    counts = defaultdict(Counter)
    for r in (
        awarded.filter(pl.col("date_awarded").dt.year() == year)
        .group_by(["pax_id", "achievement_id"])
        .agg(pl.col("id").count().alias("count"))
        .iter_rows(named=True)
    ):
        counts[r["pax_id"]].update({r["achievement_id"]: r["count"]})
    return counts


def _send_slack_message(client: WebClient, channel: str, message: str, add_reaction: bool = True) -> None:
    """Send a message to Slack with retry logic for rate limiting."""
    try:
        response = client.chat_postMessage(channel=channel, text=message, link_names=True)
        if add_reaction:
            client.reactions_add(channel=channel, name="fire", timestamp=response.get("ts"))
    except SlackApiError as e:
        if e.response.status_code == 429:
            delay = int(e.response.headers["Retry-After"])
            logging.info(f"Pausing Slack notifications for {delay} seconds.")
            time.sleep(delay)
            response = client.chat_postMessage(channel=channel, text=message, link_names=True)
            if add_reaction:
                client.reactions_add(channel=channel, name="fire", timestamp=response.get("ts"))
        else:
            raise


def _format_achievement_message(
    record: tuple, new_award_name: str, new_award_verb: str, total_achievements: int, total_idx_achievements: int
) -> str:
    """Format the achievement message for Slack."""
    ending = ordinal_suffix(total_idx_achievements)
    if record[3] == 13 and total_idx_achievements > 1:
        return f"Nice work brother. You just earned 6 pack for the {total_idx_achievements}{ending} time this year!"

    return "".join(
        [
            f"Congrats to our man <@{record[3]}>! ",
            f"He just unlocked the achievement *{new_award_name}* for {new_award_verb} ",
            f"which he earned on {record[2]}. ",
            f"This is achievement #{total_achievements} for <@{record[3]}> and the {total_idx_achievements}{ending} ",
            "time this year he's earned this award. Keep up the good work!",
        ]
    )


def send_to_slack(
    schema: str,
    token: str,
    channel: str,
    year: int,
    awarded: pl.DataFrame,
    awards: pl.DataFrame,
    dfs: list[pl.DataFrame],
    paxminer_log_channel: str,
) -> pl.DataFrame:
    """Process and send achievement notifications to Slack."""
    client = slack_client(token)
    data_to_upload = pl.DataFrame()
    achievement_counts = _get_achievement_counts(awarded, year)

    for idx, df in enumerate(dfs, start=1):
        if df.is_empty():
            try:
                award_name = awards.filter(pl.col("id") == idx).select(pl.col("name")).to_series().to_list()[0]
                logging.info(f"No data in {award_name} for {schema}")
            except IndexError:
                logging.error(f"{schema} doesn't have achievement {idx} in their awards_list table.")
            continue

        new_data = _check_for_new_results(schema, year, idx, df, awarded)
        if new_data.is_empty() or idx not in awards.select(pl.col("id")).to_series().to_list():
            continue

        # Process new achievements
        for record in new_data.iter_rows():
            achievement_counts[record[3]].update({idx: 1})
            try:
                new_award_name = awards.filter(pl.col("id") == idx).select(pl.first("name")).item()
                new_award_verb = awards.filter(pl.col("id") == idx).select(pl.first("verb")).item()

                message = _format_achievement_message(
                    record,
                    new_award_name,
                    new_award_verb,
                    achievement_counts[record[3]].total(),
                    achievement_counts[record[3]][idx],
                )

                # Send to direct message for 6-pack achievements after first one
                target_channel = record[3] if idx == 13 and achievement_counts[record[3]][idx] > 1 else channel
                _send_slack_message(client, target_channel, message)
                logging.info(f"Successfully sent slack message for {record[3]} and achievement {idx}")

            except SlackApiError as e:
                logging.error(f"Error sending achievement {new_award_name} for {schema}: {str(e)}")
                continue

        # Update data to upload
        data_to_upload = pl.concat(
            [
                data_to_upload,
                new_data.with_columns(pl.lit(idx).alias("achievement_id")).select(
                    "achievement_id", "pax_id", "date_awarded"
                ),
            ]
        )

    # Send summary message
    try:
        summary = (
            f"Successfully ran today's Weaselbot achievements patch. Sent {data_to_upload.shape[0]} new achievements."
        )
        _send_slack_message(client, paxminer_log_channel, summary, add_reaction=False)
    except SlackApiError as e:
        error_message = (
            e.response.get("response_metadata", {}).get("messages")
            if e.response.get("error") == "invalid_arguments"
            else e.response.get("error")
        )
        logging.error(
            f"Error sending Weaselbot runtime message to {paxminer_log_channel} for {schema}: {error_message}"
        )

    logging.info(f"Sent all achievement Slack messages to {schema}")
    return data_to_upload
