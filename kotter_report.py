#!/usr/bin/env /Users/jamessheldon/Library/Caches/pypoetry/virtualenvs/weaselbot-93dzw48B-py3.12/bin/python

import logging
from datetime import date, datetime, timedelta
from typing import Tuple

import polars as pl
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import MetaData, Subquery, Table
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Selectable, and_, case, func, literal_column, or_, select, union_all

from utils import mysql_connection, slack_client

# NO_Q_THRESHOLD_POSTS = 4


def home_region_sub_query(u: Table, a: Table, b: Table, ao: Table, date_range: int) -> Subquery[Tuple[str, int]]:
    """
    Abstract the subquery needed for length of time to look back for considering the home region. This is
    needed because there are many scenarios where a man could lapse in attending F3. Many different checks
    should be considered before defaulting to the maximium date range.
    """
    subquery = select(u.c.email, func.count(a.c.user_id).label("attendance"))
    subquery = subquery.select_from(
        u.join(a, a.c.user_id == u.c.user_id)
        .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
        .join(ao, b.c.ao_id == ao.c.channel_id)
    )
    subquery = subquery.where(func.datediff(func.curdate(), b.c.bd_date) < date_range)
    subquery = subquery.group_by(u.c.email).subquery()
    return subquery


def build_home_regions(schemas: pl.DataFrame, metadata: MetaData, engine: Engine) -> Selectable[Tuple[str, str, str]]:
    """
    Construct on-the-fly home regions. The current process is a UNION ALL over all regions together. By
    considering the email address, a man that posts in many different regions will have many different
    Slack IDs. However, presuming this man posts primarily in his home region, the number of
    instances of posts in the home region will be greater than that in the DR region. There are edge
    cases with no good solution. For instance, if a man moves, posts a few times and then stops. He'll
    still reflect his home region being his former home region until the end of the year.
    There's no perfect mechanism to account for this and some mis-assignments will occur.
    """
    queries = []
    for row in schemas.iter_rows():
        schema = row[0]
        if schema in ("f3devcommunity", "f3development"):
            continue
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            s1, s2, s3, s4 = (home_region_sub_query(u, a, b, ao, date_range) for date_range in (30, 60, 90, 120))

            sql = select(
                literal_column(f"'{schema}'").label("region"),
                u.c.email,
                u.c.user_id,
                case(
                    (s1.c.attendance != None, s1.c.attendance),
                    (s2.c.attendance != None, s2.c.attendance),
                    (s3.c.attendance != None, s3.c.attendance),
                    (s4.c.attendance != None, s4.c.attendance),
                    else_=func.count(a.c.user_id),
                ).label("attendance"),
            )
            sql = sql.select_from(
                u.join(a, a.c.user_id == u.c.user_id)
                .join(b, and_(a.c.q_user_id == b.c.q_user_id, a.c.ao_id == b.c.ao_id, a.c.date == b.c.bd_date))
                .join(ao, b.c.ao_id == ao.c.channel_id)
                .outerjoin(s1, u.c.email == s1.c.email)
                .outerjoin(s2, u.c.email == s2.c.email)
                .outerjoin(s3, u.c.email == s3.c.email)
                .outerjoin(s4, u.c.email == s4.c.email)
            )
            sql = sql.where(func.year(b.c.bd_date) == func.year(func.curdate()))
            sql = sql.group_by(literal_column(f"'{schema}'").label("region"), u.c.email, u.c.user_id)
            queries.append(sql)
        except Exception:
            print(f"Schema {schema} error.")

    return union_all(*queries)


def nation_sql(
    schemas: pl.DataFrame, engine: Engine, metadata: MetaData
) -> Selectable[Tuple[str, str, str, str, str, str]]:
    queries = []
    for row in schemas.iter_rows():
        schema = row[0]
        if schema in ("f3devcommunity", "f3development"):
            continue
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            sql = select(
                u.c.email,
                a.c.ao_id,
                ao.c.ao.label("ao"),
                b.c.bd_date.label("date"),
                case((or_(a.c.user_id == b.c.q_user_id, a.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag"),
            )
            sql = sql.select_from(
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

            sql = sql.where(
                b.c.bd_date > 0,
                b.c.bd_date <= func.curdate(),
                u.c.email != "none",
                u.c.user_name != "PAXminer",
                b.c.q_user_id != None,
            )
            queries.append(sql)
        except Exception:
            print(f"Schema {schema} error.")

    return union_all(*queries)


def build_kotter_report(df_posts: pl.DataFrame, df_qs: pl.DataFrame, df_noqs: pl.DataFrame, siteq: str) -> str:
    """
    Build Slack SiteQ message

    :param df_posts: All men that have not posted within the threshold
    :type df_posts: pl.DataFrame
    :param df_qs: All men that have not Q'ed within the threshold
    :type df_qs: pl.DataFrame
    :param siteq: The SlackID of the siteq for a given AO
    :type siteq: str
    :return: The multi-line string message to send across Slack to SiteQ and Weaselshaker
    :rtype: str
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
        sMessage.append("\n\nThe following PAX haven't posted in a bit. ")
        sMessage.append("Now may be a good time to reach out to them when you get a minute. No OYO! :muscle:")

        for row in df_posts.iter_rows():
            sMessage.append(f"\n- <@{row[0]}> last posted {row[2]}")

    if df_qs.height > 0:
        sMessage.append("\n\nThese guys haven't Q'd anywhere in a while (or at all!):")

        df_qs = (
            df_qs.with_columns(pl.Series([date.today()]).alias("today"))
            .with_columns(pl.col("today").sub(pl.col("date")))
            .sort("today")
            .with_columns(pl.col("date").dt.strftime("%B %d, %Y"))
        )

        for row in df_qs.iter_rows():
            sMessage.append(f"\n- <@{row[0]}> hasn't been Q since {row[2]}. That's {row[5].days} days!")

    if df_noqs.height > 0:
        for row in df_noqs.iter_rows():
            sMessage.append(f"\n- <@{row[0]}> (no Q yet!)")

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
    Loop through site-qs that have PAX on the list and send the weaselbot report.
    Then send the overall message.
    """
    for row in siteq_df.iter_rows():
        siteq = row[-1]
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
    Send a message to the paxminer_logs channel notifying everyone that the Kotter Report
    has been run.
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
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    engine = mysql_connection()
    metadata = MetaData()
    uri = engine.url.render_as_string(hide_password=False).replace("+mysqlconnector", "")

    schemas = pl.read_database_uri("SELECT schema_name FROM paxminer.regions WHERE schema_name LIKE 'f3%'", uri=uri)

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
            query = f"SELECT default_siteq, slack_token, NO_POST_THRESHOLD, NO_Q_THRESHOLD_WEEKS, REMINDER_WEEKS FROM, NO_Q_THRESHOLD_POSTS weaselbot.regions WHERE paxminer_schema = '{schema}'"
            (
                default_siteq,
                slack_token,
                NO_POST_THRESHOLD,
                NO_Q_THRESHOLD,
                REMINDER_WEEKS,
                NO_Q_THRESHOLD_POSTS,
            ) = pl.read_database_uri(query=query, uri=uri).row(0)
        except Exception as e:
            # if the site_q_user_id column isn't in their ao table, they're not set up for Kotter reports. We can stop here.
            logging.error(f"{schema}: {e}")
            continue
        df = nation_df.filter(pl.col("region") == schema)

        df = df.join(
            df.filter(
                pl.col("date") > datetime.combine(date.today() + timedelta(weeks=-REMINDER_WEEKS), datetime.min.time())
            )
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
                pl.col("date")
                < datetime.combine(date.today() + timedelta(weeks=-NO_POST_THRESHOLD), datetime.min.time())
            )
            .join(siteq_df, how="left", on="home_ao")
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
                pl.col("date") < datetime.combine(date.today() + timedelta(weeks=-NO_Q_THRESHOLD), datetime.min.time())
            )
            .join(siteq_df, how="left", on="home_ao")
            .drop("email")
            .sort("date", descending=True)
        )

        # men that have never been Q
        # data filtered for the time period. May have been Q prior.
        df_noq = (
            df.join(
                df.group_by("email", "user_id")
                .agg(pl.col("q_flag").sum())
                .filter(
                    (pl.col("q_flag") == 0)
                    & (
                        pl.col("date")
                        < datetime.combine(date.today() + timedelta(weeks=-NO_Q_THRESHOLD_POSTS), datetime.min.time())
                    )
                )
                .drop("q_flag"),
                on="email",
            )
            .select("email", "user_id", "home_ao")
            .unique()
            .join(siteq_df, how="left", on="home_ao")
            .drop("email")
        )

        client = slack_client(slack_token)
        send_weaselbot_report(schema, client, siteq_df, df_mia, df_lowq, df_noq, default_siteq)
        slack_log(schema, engine, metadata, client)

    engine.dispose()


if __name__ == "__main__":
    main()
