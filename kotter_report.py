#!/usr/bin/env /home/epetz/.cache/pypoetry/virtualenvs/weaselbot-7wWSi8jP-py3.11/bin/python3.11

import logging
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Selectable, and_, case, func, or_, select

from utils import mysql_connection, slack_client

NO_POST_THRESHOLD = 2
REMINDER_WEEKS = 2
HOME_AO_CAPTURE = datetime.combine(date.today() + timedelta(weeks=-8), datetime.min.time())
NO_Q_THRESHOLD_WEEKS = 4
NO_Q_THRESHOLD_POSTS = 4


def col_cleaner(s: pd.Series) -> pd.Series:
    """
    Simple function to apply string stripping to a pandas Series object
    """
    try:
        return s.str.strip()
    except AttributeError:
        return s


def build_kotter_report(df_posts: pd.DataFrame, df_qs: pd.DataFrame, siteq: str) -> str:
    """
    Build Slack SiteQ message

    :param df_posts: All men that have not posted within the threshold
    :type df_posts: pd.DataFrame
    :param df_qs: All men that have not Q'ed within the threshold
    :type df_qs: pd.DataFrame
    :param siteq: The SlackID of the siteq for a given AO
    :type siteq: str
    :return: The multi-line string message to send across Slack to SiteQ and Weaselshaker
    :rtype: str
    """

    siteq = f"@{siteq}" if siteq[0].upper() == "U" else "!channel"
    sMessage = [
        f"Howdy, <{siteq}>! This is your weekly WeaselBot Site Q report. According to my records...",
    ]

    if len(df_posts) > 0:
        sMessage.append("\n\nThe following PAX haven't posted in a bit.")
        sMessage.append("Now may be a good time to reach out to them when you get a minute. No OYO! :muscle:")

        for row in df_posts.itertuples(index=False):
            sMessage.append(f"\n- <@{row.pax_id}>")

    if len(df_qs) > 0:
        sMessage.append("\n\nThese guys haven't Q'd anywhere in a while (or at all!):")

        for row in df_qs.itertuples(index=False):
            sMessage.append(f"\n- <@{row.pax_id}>")
            if isinstance(row.days_since_last_q, type(pd.NA)):
                sMessage.append(" (no Q yet!)")
            else:
                sMessage.append(f" ({row.days_since_last_q} days since last Q)")
    return "".join(sMessage)


def nation_select(metadata: MetaData) -> Selectable:
    """
    SQL abstraction for pulling The Nation data from Beaker's tables.

    :param metadata: collection of all table data
    :type metadata: SQLAlchemy MetaData object
    :return: SQLAlchemy SELECT statement
    :rtype: SQLAlchemy.sql.Select object
    """
    bd = metadata.tables["weaselbot.combined_attendance"].alias("bd")
    u = metadata.tables["weaselbot.combined_users"].alias("u")
    b = metadata.tables["weaselbot.combined_beatdowns"].alias("b")
    a = metadata.tables["weaselbot.combined_aos"].alias("a")

    q_flag = case((or_(bd.c.user_id == b.c.q_user_id, bd.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag")

    sql = select(
        u.c.email,
        a.c.ao_id,
        a.c.ao_name.label("ao"),
        b.c.bd_date.label("date"),
        func.year(b.c.bd_date).label("year_num"),
        func.month(b.c.bd_date).label("month_num"),
        func.week(b.c.bd_date).label("week_num"),
        func.day(b.c.bd_date).label("day_num"),
        q_flag,
    )
    sql = sql.select_from(
        bd.join(u, u.c.user_id == bd.c.user_id)
        .join(b, bd.c.beatdown_id == b.c.beatdown_id)
        .join(a, b.c.ao_id == a.c.ao_id)
    )
    return sql.where(and_(b.c.bd_date > 0, b.c.bd_date <= func.curdate()))


def region_select(metadata: MetaData) -> Selectable:
    """
    SQL abstraction for selecting all regions that want to see Weaselbot reports.

    This data comes from the main weaselbot schema
    :param metadata: collection of all table data
    :type metadata: SQLAlchemy MetaData object
    :return: SQLAlchemy SELECT statement
    :rtype: SQLAlchemy.sql.Select object
    """
    r = metadata.tables["weaselbot.regions"]
    return select(r).where(r.c.send_aoq_reports == 1)


def region_df(sql: Selectable, engine: Engine) -> pd.DataFrame:
    """
    Execute the provided SQL statement and return the cleaned dataframe

    :param sql: SQLAlchemy abstracted SQL query
    :type sql: SQLAlchemy.sql.Select object
    :param engine: database connection engine
    :type engine: SQLAlchemy.Engine
    :return: DataFrame of all regions that want to see data and their siteQ's
    :rtype: pd.DataFrame
    """
    dtypes = {
        "id": pd.Int16Dtype(),
        "paxminer_schema": pd.StringDtype(),
        "slack_token": pd.StringDtype(),
        "send_achievements": pd.Int16Dtype(),
        "send_aoq_reports": pd.Int16Dtype(),
        "achievement_channel": pd.StringDtype(),
        "default_siteq": pd.StringDtype(),
    }
    df = pd.read_sql(sql, engine, dtype=dtypes)
    df = df.apply(col_cleaner)

    return df


def nation_df(sql: Selectable, engine: Engine) -> pd.DataFrame:
    """
    Execute the provided SQL statement and return the cleaned dataframe

    :param sql: SQLAlchemy abstracted SQL query
    :type sql: SQLAlchemy.sql.Select object
    :param engine: database connection engine
    :type engine: SQLAlchemy.Engine
    :return: DataFrame of the entire regions workout data for the current year
    :rtype: pd.DataFrame
    """
    dtypes = {
        "email": pd.StringDtype(),
        "ao_id": pd.StringDtype(),
        "ao": pd.StringDtype(),
        "year_num": pd.Int16Dtype(),
        "month_num": pd.Int16Dtype(),
        "week_num": pd.Int16Dtype(),
        "day_num": pd.Int16Dtype(),
        "q_flag": pd.Int16Dtype(),
    }
    df = pd.read_sql(sql, engine, parse_dates="date", dtype=dtypes)
    df = df.apply(col_cleaner)
    df.email = df.email.str.lower().replace("none", pd.NA)

    return df


def pull_region_users(row: tuple[Any, ...], engine: Engine, metadata: MetaData) -> pd.DataFrame:
    """
    Pull the users for the specific region defined in `row`.

    :param row: key: value pair of data elements from a pandas dataframe row.
    :type row: named tuple
    :param engine: database connection engine
    :type engine: SQLAlchemy.Engine
    :param metadata: metadata object of tables
    :type metadata: SQLAlchemy.MetaData
    :return: dataframe of users
    :rtype: pd.DataFrame
    """
    users = Table("users", metadata, autoload_with=engine, schema=row.paxminer_schema)
    df = pd.read_sql(select(users), engine, dtype=pd.StringDtype(), parse_dates="start_date")
    df.app = df.app.astype(pd.Int64Dtype())
    df = df.apply(col_cleaner)
    df.email = df.email.str.lower().replace("none", pd.NA)
    df.phone = df.phone.replace("", pd.NA)

    return df


def add_home_ao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Insert the `home_ao` to the data. This is critical to making sure posts land in the
    proper Slack channel.

    :param df: dataframe of users
    :type df; pd.DataFrame
    :return: dataframe of users with home region included
    :rtype: pd.DataFrame
    """
    home_ao_df = (
        df.loc[df.date > HOME_AO_CAPTURE]
        .groupby(["pax_id"])["ao"]
        .value_counts()
        .groupby(level=0)
        .head(1)
        .reset_index()
        .drop("count", axis=1)
        .rename({"ao": "home_ao"}, axis=1)
    )

    return pd.merge(df, home_ao_df, how="left").dropna(subset="home_ao")


def pax_appearances(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a dataframe of pax appearances at beatdowns over time. This is used to determine
    rolling-n dates for how long it's been since a man has shown up.

    :param df: dataframe of men's attendance
    :type df: pd.DataFrame
    :return: date-transformed dataframe outlining the last time a man posted to an AO
    :rtype: pd.DataFrame
    """
    df1 = df.groupby(["year_num", "week_num"], as_index=False)["date"].min()
    df2 = df.groupby(["pax_id", "home_ao"], as_index=False)["ao"].count()
    return pd.merge(df2, df1, how="cross").drop("ao", axis=1)


def clean_data(
    df: pd.DataFrame, row: tuple[Any, ...], engine: Engine, metadata: MetaData
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Take a region's data and review it against the nation data to determine which men in the given
    region need to be counted toward a Kotter report. This step is the main transformation step
    to identify men for the report.

    :param df: the Nation data from Weaselbot
    :type df: pd.DataFrame
    :param row: the specific region being focused on
    :type row: named tuple
    :type engine: SQLAlchemy.Engine
    :param metadata: metadata object of tables
    :type metadata: SQLAlchemy.MetaData
    :return: three dataframes for the three different types of identification we're making
    :rtype: tuple of pd.DataFrame objects
    """
    df.rename({"user_id": "pax_id", "user_name": "pax_name"}, axis=1, inplace=True)

    df = add_home_ao(df)

    df_non_zero = (
        df.groupby(["year_num", "week_num", "pax_id", "home_ao"], as_index=False)
        .agg({"day_num": np.count_nonzero})
        .rename(columns={"day_num": "post_count"})
    )
    df_appearances = pax_appearances(df)
    df6 = (
        pd.merge(df_appearances, df_non_zero, how="left")
        .dropna(
            subset=[
                "date",
            ]
        )
        .fillna(pd.NA)
        .sort_values(["pax_id", "date"])
        .assign(
            post_count_rolling=lambda x: x.post_count.rolling(NO_POST_THRESHOLD, min_periods=1).sum(),
            post_count_rolling_stop=lambda x: x.post_count.rolling(
                NO_POST_THRESHOLD + REMINDER_WEEKS, min_periods=1
            ).sum(),
        )
    )
    df6.post_count_rolling = df6.post_count_rolling.astype(pd.Float64Dtype())
    df6.post_count_rolling_stop = df6.post_count_rolling_stop.astype(pd.Float64Dtype())

    # Pull list of guys not posting
    pull_week = df6.loc[df6.date < datetime.today()][
        "date"
    ].max()  # this will only work as expected if you run on Sunday because of the rolling count
    df7 = df6[(df6["post_count_rolling"].isna()) & (df6["date"] == pull_week) & (df6["post_count_rolling_stop"] > 0)]

    # Pull list of guys not Q-ing
    df8 = (
        df.loc[df["q_flag"] == True]  # noqa: E712
        .groupby(["pax_id"], as_index=False)["date"]
        .max()
        .rename(columns={"date": "last_q_date"})
        .assign(days_since_last_q=lambda x: (datetime.today() - x.last_q_date).dt.days)
    )
    df8.days_since_last_q = df8.days_since_last_q.astype(pd.Int64Dtype())
    df9 = pd.merge(df6, df8, how="left")
    df10 = df9[
        (df9["post_count_rolling"] > 0)
        & (df6["date"] == pull_week)
        & (
            (df9["days_since_last_q"] > (NO_Q_THRESHOLD_WEEKS * 7))
            | (df9["days_since_last_q"].isna() & (df9["post_count_rolling"] > NO_Q_THRESHOLD_POSTS))
        )
    ]

    # Merge siteq list
    ao = Table("aos", metadata, autoload_with=engine, schema=row.paxminer_schema)

    df_siteq = pd.read_sql(select(ao.c.ao, ao.c.site_q_user_id), engine, dtype=pd.StringDtype()).dropna()
    df_posts = pd.merge(df7, df_siteq, how="left", left_on="home_ao", right_on="ao")
    df_posts = df_posts.loc[df_posts.home_ao.notna()]

    df_qs = pd.merge(
        df10, df_siteq, how="left", left_on="home_ao", right_on="ao"
    )  # remove NAs... these are guys who haven't posted to a regular AO in the home_ao period

    for df_ in [df_posts, df_qs]:
        mask = df_["site_q_user_id"].isna()
        df_.loc[mask, "site_q_user_id"] = row.default_siteq

    return df_siteq, df_posts, df_qs


def send_weaselbot_report(
    row: tuple[Any, ...],
    client: WebClient,
    df_siteq: pd.DataFrame,
    df_posts: pd.DataFrame,
    df_qs: pd.DataFrame,
) -> None:
    """
    Produce and post the Slack notification to the site Q's and default user.

    :param row: specific region we're focusing on.
    :type row: named tuple
    :param client: Slack client
    :type client: slack_api.WebClient
    :param df_siteq: dataframe containing all the region site_q Slack ID's
    :type df_siteq: pd.DataFrame
    :param df_posts: dataframe containing all the posts made by region pax
    :type df_posts: pd.DataFrame
    :param df_qs: dataframe containing all the Q's performed by region pax
    :type df_qs: pd.DataFrame
    :return: None
    :rtype: NoneType
    """
    # Loop through site-qs that have PAX on the list and send the weaselbot report
    # If df_siteq is empty, this block doesn't run.
    for siteq in df_siteq["site_q_user_id"].unique():
        dftemp_posts = df_posts[df_posts["site_q_user_id"] == siteq]
        dftemp_qs = df_qs[df_qs["site_q_user_id"] == siteq]

        # Build message
        sMessage = build_kotter_report(dftemp_posts, dftemp_qs, siteq)

        # Send message
        if (len(dftemp_posts) + len(dftemp_qs)) > 0:
            try:
                client.chat_postMessage(channel=siteq, text=sMessage, link_names=True)
                logging.info(f"Sent {siteq} this message:\n\n{sMessage}\n\n")
            except Exception as e:
                logging.error(f"Error sending message to {siteq} {e}")

    sMessage = build_kotter_report(df_posts, df_qs, row.default_siteq)
    sMessage += "\n\nNote: If you have listed your site Qs on your aos table, this information will have gone out to them as well."
    try:
        if row.default_siteq not in df_siteq["site_q_user_id"].unique().tolist():
            client.chat_postMessage(channel=row.default_siteq, text=sMessage, link_names=True)
            logging.info(f"Sent {row.default_siteq} this message:\n\n{sMessage}\n\n")
    except SlackApiError as e:
        logging.error(f"hit exception {e}")
        logging.error(e.response)
        if e.response.get("error") == "not_in_channel":
            try:
                logging.info("trying to join channel")
                client.conversations_join(channel=row.default_siteq)
                client.chat_postMessage(channel=row.default_siteq, text=sMessage, link_names=True)
                logging.info(f"sent this message:\n\n{sMessage}\n\n")
            except Exception as e:
                logging.error("hit exception joining channel")


def notify_yhc(row: tuple[Any], engine: Engine, metadata: MetaData, client: WebClient) -> None:
    ao = metadata.tables[f"{row.paxminer_schema}.aos"]
    with engine.begin() as cnxn:
        paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
    try:
        client.chat_postMessage(channel=paxminer_log_channel, text="Successfully sent kotter reports")
        logging.info(f"Sent {paxminer_log_channel} this message:\n\nSuccessfully sent kotter reports\n\n")
    except SlackApiError as e:
        logging.error(f"Error sending message to {paxminer_log_channel}: {e}")  # TODO: add self to channel
    logging.info("All done!")


def main():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")

    nation_sql, region_sql = nation_select(metadata), region_select(metadata)

    df_nation, df_regions = nation_df(nation_sql, engine), region_df(region_sql, engine)

    for row in df_regions.itertuples(index=False):
        logging.info(f"running {row.paxminer_schema}...")
        user_df = pull_region_users(row, engine, metadata)
        df = pd.merge(df_nation, user_df, how="inner", on="email").dropna(subset="email")

        df_siteq, df_posts, df_qs = clean_data(df, row, engine, metadata)

        token = row.slack_token
        client = slack_client(token)
        # send_weaselbot_report(row, client, df_siteq, df_posts, df_qs)

        notify_yhc(row, engine, metadata, client)

    engine.dispose()


if __name__ == "__main__":
    main()
