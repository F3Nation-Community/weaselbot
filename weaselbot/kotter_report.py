#!/usr/bin/env ./.venv/bin/python

"""
This module contains functions to generate and send Kotter reports for different regions using SQLAlchemy and Slack SDK.
The main functionalities include:
1. Generating subqueries to retrieve user attendance data.
2. Building SQL queries to retrieve home region attendance data.
3. Generating SQL queries to retrieve user attendance and beatdown information.
4. Building a weekly report message for WeaselBot Site Q.
5. Sending the generated report to specified site Q users via Slack.
6. Logging the successful sending of Kotter reports to a Slack channel.
Functions:
    home_region_sub_query(u: Table, a: Table, b: Table, ao: Table, date_range: int) -> Subquery[Tuple[str, int]]:
    build_home_regions(schemas: pl.DataFrame, metadata: MetaData, engine: Engine) -> Selectable[Tuple[str, str, str]]:
    nation_sql(schemas: pl.DataFrame, engine: Engine, metadata: MetaData) -> Selectable[Tuple[str, str, str, str, str, str]]:
    build_kotter_report(df_posts: pl.DataFrame, df_qs: pl.DataFrame, df_noqs: pl.DataFrame, siteq: str) -> str:
    send_weaselbot_report(schema: str, client: WebClient, siteq_df: pl.DataFrame, df_mia: pl.DataFrame, df_lowq: pl.DataFrame, df_noq: pl.DataFrame, default_siteq: str) -> None:
    slack_log(schema: str, engine: Engine, metadata: MetaData, client: WebClient) -> None:
    main() -> None:
"""

import logging
from datetime import date, timedelta
from typing import Tuple

import polars as pl
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import MetaData, Subquery, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Selectable, and_, case, func, literal_column, or_, select, union_all

from .utils import mysql_connection, slack_client


def home_region_sub_query(u: Table, a: Table, b: Table, ao: Table, date_range: int) -> Subquery[Tuple[str, int]]:
    """
    Generates a subquery to retrieve user email and their attendance count within a specified date range.
    Args:
        u (Table): The user table.
        a (Table): The attendance table.
        b (Table): The bridge table linking attendance and another entity.
        ao (Table): The table containing additional information about the entity.
        date_range (int): The number of days to look back from the current date.
    Returns:
        Subquery[Tuple[str, int]]: A subquery object that returns tuples of user email and attendance count.
    """

    return (
        select(u.c.email, func.count(a.c.user_id).label("attendance"))
        .select_from(
            u.join(a, a.c.user_id == u.c.user_id)
            .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
            .join(ao, b.c.ao_id == ao.c.channel_id)
        )
        .where(func.datediff(func.curdate(), b.c.bd_date) < date_range)
        .group_by(u.c.email)
        .subquery()
    )


def build_home_regions(schemas: pl.DataFrame, metadata: MetaData, engine: Engine) -> Selectable[Tuple[str, str, str]]:
    """
    Builds a SQL query to retrieve home region attendance data for users across multiple schemas.
    Args:
        schemas (pl.DataFrame): A DataFrame containing schema names.
        metadata (MetaData): SQLAlchemy MetaData object.
        engine (Engine): SQLAlchemy Engine object.
    Returns:
        Selectable[Tuple[str, str, str]]: A union of SQL queries for each schema, selecting region, user email, user ID, and attendance.
    The function iterates over each schema, constructs tables for users, attendance, beatdowns, and AOs, and builds subqueries for different date ranges.
    It then constructs a main query to select the region, user email, user ID, and attendance, joining the necessary tables and subqueries.
    The queries are combined using a union_all operation and returned.
    If an error occurs while processing a schema, it logs the error and continues with the next schema.
    """

    queries = []
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            s1, s2, s3, s4 = (home_region_sub_query(u, a, b, ao, date_range) for date_range in (30, 60, 90, 120))

            sql = (
                select(
                    literal_column(f"'{schema}'").label("region"),
                    u.c.email,
                    u.c.user_id,
                    case(
                        (s1.c.attendance.is_not(None), s1.c.attendance),
                        (s2.c.attendance.is_not(None), s2.c.attendance),
                        (s3.c.attendance.is_not(None), s3.c.attendance),
                        (s4.c.attendance.is_not(None), s4.c.attendance),
                        else_=func.count(a.c.user_id),
                    ).label("attendance"),
                )
                .select_from(
                    u.join(a, a.c.user_id == u.c.user_id)
                    .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
                    .join(ao, b.c.ao_id == ao.c.channel_id)
                    .outerjoin(s1, u.c.email == s1.c.email)
                    .outerjoin(s2, u.c.email == s2.c.email)
                    .outerjoin(s3, u.c.email == s3.c.email)
                    .outerjoin(s4, u.c.email == s4.c.email)
                )
                .where(func.year(b.c.bd_date) == func.year(func.curdate()))
                .group_by(literal_column(f"'{schema}'").label("region"), u.c.email, u.c.user_id)
            )
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def nation_sql(
    schemas: pl.DataFrame, engine: Engine, metadata: MetaData
) -> Selectable[Tuple[str, str, str, str, str, str]]:
    """
    Generates a SQL query to retrieve user attendance and beatdown information from multiple schemas.
    Args:
        schemas (pl.DataFrame): A DataFrame containing schema names.
        engine (Engine): SQLAlchemy Engine object for database connection.
        metadata (MetaData): SQLAlchemy MetaData object for schema reflection.
    Returns:
        Selectable[Tuple[str, str, str, str, str, str]]: A union of SQL queries for each schema, selecting user email,
        AO ID, AO name, beatdown date, and a flag indicating if the user was a Q (leader) for the beatdown.
    The function iterates over each schema, constructs a SQL query to join the 'users', 'bd_attendance', 'beatdowns',
    and 'aos' tables, and applies necessary filters. If an error occurs during query construction for a schema,
    it logs the error and continues with the next schema.
    """
    queries = []
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            sql = (
                select(
                    u.c.email,
                    a.c.ao_id,
                    ao.c.ao.label("ao"),
                    b.c.bd_date.label("date"),
                    case((or_(a.c.user_id == b.c.q_user_id, a.c.user_id == b.c.coq_user_id), 1), else_=0).label(
                        "q_flag"
                    ),
                )
                .select_from(
                    u.join(a, a.c.user_id == u.c.user_id)
                    .join(
                        b,
                        and_(
                            or_(a.c.q_user_id == b.c.q_user_id, a.c.q_user_id == b.c.coq_user_id),
                            a.c.ao_id == b.c.ao_id,
                            a.c.date == b.c.bd_date,
                        ),
                    )
                    .join(ao, b.c.ao_id == ao.c.channel_id)
                )
                .where(
                    b.c.bd_date > 0,
                    b.c.bd_date <= func.curdate(),
                    u.c.email != "none",
                    u.c.user_name != "PAXminer",
                    b.c.q_user_id.is_not(None),
                )
            )
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def build_kotter_report(df_posts: pl.DataFrame, df_qs: pl.DataFrame, df_noqs: pl.DataFrame, siteq: str) -> str:
    """
    Generates a weekly report message for WeaselBot Site Q.
    Args:
        df_posts (pl.DataFrame): DataFrame containing users who haven't posted in a while.
        df_qs (pl.DataFrame): DataFrame containing users who haven't Q'd in a while.
        df_noqs (pl.DataFrame): DataFrame containing users who have never Q'd.
        siteq (str): Site Q identifier, used to determine the mention format.
    Returns:
        str: The generated report message.
    """

    try:
        siteq = f"@{siteq}" if siteq[0].upper() == "U" else "!channel"
    except (IndexError, TypeError):
        logging.error("No Site Q in the table. Proceeding with @channel")
        siteq = "!channel"
    sMessage = [
        f"Howdy, <{siteq}>! This is your weekly WeaselBot Site Q report. According to my records...",
    ]

    if df_posts.height > 0:
        sMessage.append("\n\nThe following men haven't posted in a while.")

        for row in df_posts.iter_rows(named=True):
            sMessage.append(f"\n- <@{row['user_id']}> last posted {row['date']}")

    if df_qs.height > 0:
        sMessage.append("\n\nThese guys haven't Q'd in a while. Here's how many days it's been:")

        df_qs = (
            df_qs.with_columns(pl.lit(date.today()).alias("today"))
            .with_columns(pl.col("today").sub(pl.col("date")))
            .sort("today")
            .with_columns(pl.col("date").dt.strftime("%B %d, %Y"))
        )

        for row in df_qs.iter_rows():
            sMessage.append(f"\n- <@{row[0]}>: {row[5].days}!")

    if df_noqs.height > 0:
        sMessage.append("\n\nThese guys have never been Q:")
        for row in df_noqs.iter_rows(named=True):
            sMessage.append(f"\n- <@{row['user_id']}>")

    return "".join(sMessage)


def send_weaselbot_report(
    schema: str,
    client: WebClient,
    siteq_df: pl.DataFrame,
    df_mia: pl.DataFrame,
    df_lowq: pl.DataFrame,
    df_noq: pl.DataFrame,
    default_siteq: str,
) -> None:
    """
    Sends a report to specified site Q users and a default site Q user via Slack.
    Parameters:
    schema (str): The schema name for logging purposes.
    client (WebClient): The Slack WebClient instance used to send messages.
    siteq_df (pl.DataFrame): DataFrame containing site Q user IDs.
    df_mia (pl.DataFrame): DataFrame containing MIA data.
    df_lowq (pl.DataFrame): DataFrame containing low quality data.
    df_noq (pl.DataFrame): DataFrame containing no quality data.
    default_siteq (str): The default site Q user ID to send the report to if not listed in siteq_df.
    Returns:
    None
    """
    for row in siteq_df.iter_rows(named=True):
        siteq = row["site_q_user_id"]
        filter = pl.col("site_q_user_id") == siteq
        mia = df_mia.filter(filter)
        lowq = df_lowq.filter(filter)
        noq = df_noq.filter(filter)
        sMessage = build_kotter_report(mia, lowq, noq, siteq)

        if sum((mia.height, lowq.height, noq.height)) > 0:
            try:
                client.chat_postMessage(channel=siteq, text=sMessage, link_names=True)
                logging.info(f"Sent {siteq} this message:\n\n{sMessage}\n\n")
            except Exception as e:
                logging.error(f"Error sending message to {siteq} with {e}")

    sMessage = build_kotter_report(df_mia, df_lowq, df_noq, default_siteq)
    sMessage += "\n\nNote: If you have listed your site Qs on your aos table, this information will have gone out to them as well."
    try:
        if default_siteq not in siteq_df.get_column("site_q_user_id"):
            client.chat_postMessage(channel=default_siteq, text=sMessage, link_names=True)
            logging.info(f"Sent {default_siteq} this message:\n\n{sMessage}\n\n")
    except SlackApiError as e:
        if e.response.get("error") == "not_in_channel":
            try:
                logging.info("trying to join channel")
                client.conversations_join(channel=default_siteq)
                client.chat_postMessage(channel=default_siteq, text=sMessage, link_names=True)
                logging.info(f"sent this message:\n\n{sMessage}\n\n")
            except Exception as e:
                logging.error("hit exception joining channel")
        elif e.response.get("error") == "channel_not_found":
            logging.error(f"The channel or user {default_siteq} doesn't exist for {schema}.")
        else:
            logging.error(f"hit the following exception for {schema}: {e}")


def slack_log(schema: str, engine: Engine, metadata: MetaData, client: WebClient) -> None:
    """
    Sends a message to a Slack channel indicating that kotter reports have been successfully sent.
    Args:
        schema (str): The database schema to use.
        engine (Engine): The SQLAlchemy engine connected to the database.
        metadata (MetaData): The SQLAlchemy MetaData object.
        client (WebClient): The Slack WebClient used to send messages.
    Raises:
        SlackApiError: If there is an error sending the message to Slack.
    """

    ao = Table("aos", metadata, autoload_with=engine, schema=schema)
    with engine.begin() as cnxn:
        paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
    try:
        client.chat_postMessage(channel=paxminer_log_channel, text="Successfully sent kotter reports")
        logging.info(f"Sent {paxminer_log_channel} this message:\n\nSuccessfully sent kotter reports\n\n")
        logging.info("All done!")
    except SlackApiError as e:
        if e.response.get("error") == "not_in_channel":
            logging.error(
                f"Weaselbot is not added to the paxminer_logs channel for {schema}. Their admin needs to do this."
            )
        else:
            logging.error(e)
        logging.error("Finished with errors.")


def main():
    """
    Main function to generate and send Kotter reports for different regions.
    This function performs the following steps:
    1. Sets up logging configuration.
    2. Establishes a connection to the MySQL database.
    3. Retrieves the list of schemas to process.
    4. Builds SQL queries for home regions and national data.
    5. Reads data from the database and processes it to generate dataframes.
    6. Iterates through each schema to generate specific reports.
    7. Filters and processes data to identify men who haven't posted or Q'ed in a while.
    8. Sends the generated reports to Slack using the Weaselbot.
    The function handles exceptions for schemas that are not set up for Kotter reports and logs errors accordingly.
    Note: This function assumes the existence of several helper functions such as `mysql_connection`,
    `build_home_regions`, `nation_sql`, `slack_client`, `send_weaselbot_report`, and `slack_log`.
    Raises:
        Exception: If there is an error in processing a schema, it logs the error and continues with the next schema.
    """

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    engine = mysql_connection()
    metadata = MetaData()
    uri = engine.url.render_as_string(hide_password=False).replace("+mysqlconnector", "")

    schemas = pl.read_database_uri("SELECT schema_name FROM paxminer.regions WHERE schema_name LIKE 'f3%'", uri=uri)
    schemas = schemas.filter(~pl.col("schema_name").is_in(("f3devcommunity", "f3development", "f3csra")))

    home_regions_sql = str(
        build_home_regions(schemas, metadata, engine).compile(engine, compile_kwargs={"literal_binds": True})
    )
    nation_query = str(nation_sql(schemas, engine, metadata).compile(engine, compile_kwargs={"literal_binds": True}))
    logging.info("Building home regions dataframe...")
    home_regions = pl.read_database_uri(home_regions_sql, uri=uri)
    logging.info("Building national dataframe...")
    nation_df = pl.read_database_uri(nation_query, uri=uri)

    home_regions = home_regions.group_by("email").agg(pl.all().sort_by("attendance").last())
    nation_df = nation_df.join(home_regions.drop("attendance"), on="email")
    del home_regions

    for row in schemas.iter_rows():
        schema = row[0]
        logging.info(f"running {schema}...")
        try:
            query = (
                f"SELECT channel_id AS home_ao, ao, site_q_user_id FROM {schema}.aos WHERE site_q_user_id IS NOT NULL"
            )
            siteq_df = pl.read_database_uri(query=query, uri=uri)
            query = f"SELECT default_siteq, slack_token, NO_POST_THRESHOLD, NO_Q_THRESHOLD_WEEKS, REMINDER_WEEKS, NO_Q_THRESHOLD_POSTS, HOME_AO_CAPTURE FROM weaselbot.regions WHERE paxminer_schema = '{schema}'"
            (
                default_siteq,
                slack_token,
                NO_POST_THRESHOLD,
                NO_Q_THRESHOLD,
                REMINDER_WEEKS,
                NO_Q_THRESHOLD_POSTS,
                HOME_AO_CAPTURE,
            ) = pl.read_database_uri(query=query, uri=uri).row(0)
        except Exception as e:
            # if the site_q_user_id column isn't in their ao table, they're not set up for Kotter reports. We can stop here.
            logging.error(f"{schema}: {e}")
            continue
        df = nation_df.filter(pl.col("region") == schema)

        df = df.join(
            df.filter(pl.col("date") > date.today() + timedelta(weeks=-HOME_AO_CAPTURE))
            .group_by("email", "ao_id")
            .agg(pl.col("ao").count())
            .group_by("email")
            .agg(pl.all().sort_by("ao").last())
            .with_columns(pl.col("ao_id").alias("home_ao"))
            .drop("ao", "ao_id"),
            on="email",
        )

        # men that haven't posted in a while
        df_mia = (
            df.group_by("email", "user_id", "home_ao")
            .agg(pl.col("date").max())
            .filter(
                pl.col("date").is_between(
                    date.today() + timedelta(weeks=-REMINDER_WEEKS), date.today() + timedelta(weeks=-NO_POST_THRESHOLD)
                )
            )
            .join(siteq_df, how="left", on="home_ao", coalesce=True)
            .drop("email")
            .sort("date", descending=True)
            .with_columns(pl.col("date").dt.strftime("%B %d, %Y"))
        )

        # men that haven't q'ed in a while but have in the past
        df_lowq = (
            df.filter(pl.col("q_flag") == 1)
            .group_by("email", "user_id", "home_ao")
            .agg(pl.col("date").max())
            .filter(
                pl.col("date").is_between(
                    date.today() + timedelta(weeks=-REMINDER_WEEKS),
                    date.today() + timedelta(weeks=-NO_Q_THRESHOLD_POSTS),
                )
            )
            .join(siteq_df, how="left", on="home_ao", coalesce=True)
            .drop("email")
            .sort("date", descending=True)
        )
        df_lowq = df_lowq.filter(~pl.col("user_id").is_in(df_mia.get_column("user_id").to_list()))

        # men that have never been Q
        # data filtered for the time period. May have been Q prior.
        df_noq = (
            df.join(
                df.group_by("email", "user_id")
                .agg(pl.col("q_flag").sum())
                .filter(pl.col("q_flag") == 0)
                .drop("q_flag"),
                on="email",
            )
            .filter(
                pl.col("date").is_between(
                    date.today() + timedelta(weeks=-REMINDER_WEEKS), date.today() + timedelta(weeks=-NO_Q_THRESHOLD)
                )
            )
            .select("email", "user_id", "home_ao")
            .unique()
            .join(siteq_df, how="left", on="home_ao", coalesce=True)
            .drop("email")
        )
        df_noq = df_noq.filter(~pl.col("user_id").is_in(df_mia.get_column("user_id").to_list()))
        df_noq = df_noq.filter(~pl.col("user_id").is_in(df_lowq.get_column("user_id").to_list()))

        client = slack_client(slack_token)
        send_weaselbot_report(schema, client, siteq_df, df_mia, df_lowq, df_noq, default_siteq)
        slack_log(schema, engine, metadata, client)

    engine.dispose()


if __name__ == "__main__":
    main()
