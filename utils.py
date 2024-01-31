import logging
import os
import ssl
from typing import NamedTuple

import pandas as pd
from dotenv import load_dotenv
from slack_sdk import WebClient
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
            ).loc[lambda x: x._merge == 'left_only']
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
            ).loc[lambda x: x._merge == 'left_only']
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
            ).loc[lambda x: x._merge == 'left_only']


def send_to_slack(row: NamedTuple, year: int, awarded: pd.DataFrame, awards: pd.DataFrame, dfs: list[pd.DataFrame]) -> None:

    for idx, df in enumerate(dfs, start=1):
        if df.empty:
            # no one anywhere got this award. No sense wasting resources on it.
            try:
                logging.info(f"No data in {awards.loc[idx-1, 'name']} for {row.paxminer_schema}")
            except KeyError:
                logging.error(f"{row.paxminer_schema} doesn't have achievement {idx} in their awards_list table.")
            continue
        new_data = _check_for_new_results(row, year, idx, df, awarded)
        if new_data.empty:
            # there is data but nothing new since the last run. Carry on.
            logging.info(f"No new data in {awards.loc[idx-1, 'name']} for {row.paxminer_schema}")
            continue

        # we got this far so there are achievements to award.
        logging.info(f"Here's the new data in {awards.loc[idx-1, 'name']} for {row.paxminer_schema}")
        print(new_data)
    # CONTINUE WORK HERE. Implement the rest of the details below.
    # pull region achievements_list table (some regions have customized)
    # parse each df in dfs into something consumable
    # Instantiate a slack client
    # pull current accomplishments from mysql and decide what needs to be added
    # push customized slack notification
    # write new records to accomplishment table
