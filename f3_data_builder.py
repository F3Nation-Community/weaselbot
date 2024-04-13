#!/usr/bin/env python

import ast
import logging
import sys
from typing import Any, Hashable, Tuple

import pandas as pd
from pandas._libs.missing import NAType
from sqlalchemy import MetaData, Table, literal_column
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.sql import and_, func, select
from sqlalchemy.sql.expression import Insert, Selectable, Subquery

from utils import mysql_connection


def insert_statement(table: Table, insert_values: list[dict[Hashable, Any]], update_cols: Tuple[str, ...]) -> Insert:
    """
    Abstract the MySQL insert statement. Returns a SQLAlchemy INSERT statement that renders
    the following:

    ```sql
    INSERT INTO <table> (col1, col2, ...) VALUES ((val1a, val1b, ...), (val2a, val2b, ...), ...)
    AS NEW ON DUPLICATE KEY UPDATE colx = NEW(colx), coly = NEW(coly), ...
    ```

    In this way, the creation and execution of MySQL INSERT statements becomes far less error prone and more
    standardized.

    :param table: The target table for the INSERT statement
    :type table: SQLAlchemy Table object
    :param insert_values: A list of dictionaries. Each dictionary is a key / value pair preresenting
    the table column_name / value_to_insert.
    :type insert_values: list[dict[str, Any]]
    :rtype: Insert[Any]
    :return: SQLAlchemy INSERT statement
    """
    sql = insert(table).values(insert_values)
    on_dup = sql.on_duplicate_key_update({v.name: v for v in sql.inserted if v.name in update_cols})
    return on_dup


def region_subquery(metadata: MetaData) -> Subquery:
    """
    Abstracting some SQL duplication between the
    paxminer and weaselbot region queries
    """
    cb = metadata.tables["weaselbot.combined_beatdowns"]
    a = metadata.tables["weaselbot.combined_aos"]

    sql = select(
        a.c.region_id,
        func.max(cb.c.timestamp).label("max_timestamp"),
        func.max(cb.c.ts_edited).label("max_ts_edited"),
        func.count().label("beatdown_count"),
    )
    sql = sql.select_from(cb.join(a, cb.c.ao_id == a.c.ao_id))
    sql = sql.group_by(a.c.region_id).subquery("b")
    return sql


def paxminer_region_query(metadata: MetaData, cr: Table) -> Selectable:
    """
    Construct the region SQL using paxminer
    """
    r = metadata.tables["paxminer.regions"]
    sub = region_subquery(metadata)

    sql = select(
        r.c.schema_name,
        r.c.region.label("region_name"),
        sub.c.max_timestamp,
        sub.c.max_ts_edited,
        sub.c.beatdown_count,
        cr.c.region_id,
    )
    sql = sql.select_from(
        r.outerjoin(cr, r.c.schema_name == cr.c.schema_name).outerjoin(sub, cr.c.region_id == sub.c.region_id)
    )

    return sql


def weaselbot_region_query(metadata: MetaData, cr: Table) -> Selectable:
    """
    Construct the region SQL using weaselbot
    """
    sub = region_subquery(metadata)

    sql = select(cr, sub.c.beatdown_count)
    sql = sql.select_from(cr.outerjoin(sub, cr.c.region_id == sub.c.region_id))

    return sql


def region_queries(engine: Engine, metadata: MetaData) -> pd.DataFrame:
    """
    Using PAXMiner and Weaselbot region tables, make updates to the
    Weaselbot combined_regions table if any exist from PAXMiner.

    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: pandas.DataFrame
    :return: A dataframe containing current region information
    """
    cr = metadata.tables["weaselbot.combined_regions"]

    paxminer_region_sql = paxminer_region_query(metadata, cr)

    df_regions = pd.read_sql(paxminer_region_sql, engine)
    df_regions = df_regions.convert_dtypes()
    insert_values = df_regions.drop("beatdown_count", axis=1).to_dict("records")
    update_cols = ("region_name", "max_timestamp", "max_ts_edited")
    region_insert_sql = insert_statement(cr, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(region_insert_sql)

    dtypes = {
        "region_id": pd.StringDtype(),
        "region_name": pd.StringDtype(),
        "schema_name": pd.StringDtype(),
        "slack_team_id": pd.StringDtype(),
        "max_timestamp": pd.Float64Dtype(),
        "max_ts_edited": pd.Float64Dtype(),
        "beatdown_count": pd.Int16Dtype(),
    }

    weaselbot_region_sql = weaselbot_region_query(metadata, cr)
    with engine.begin() as cnxn:
        logging.info("Retrieving total region info.")
        df_regions = pd.read_sql(weaselbot_region_sql, cnxn, dtype=dtypes)
        logging.info("Done")

    return df_regions


def home_region_query(engine: Engine, metadata: MetaData) -> pd.DataFrame:
    """
    Determine the PAX home region based on a rolling 180 days. There's no exact
    science to this. Men travel, men move, men do all sorts of things. In an attempt
    to quantify a home region, the current decision is to look at the last beatdowns
    a man posted to in the most current 30 days. The most frequent is determined to
    be the home region. There are flaws to this approach but it's sound for the time
    being.

    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: pandas.DataFrame
    :return: A dataframe containing current home region information for each PAX
    """

    ud = metadata.tables["weaselbot.combined_users_dup"]
    u = metadata.tables["weaselbot.combined_users"]
    a = metadata.tables["weaselbot.combined_attendance"]
    b = metadata.tables["weaselbot.combined_beatdowns"]
    ao = metadata.tables["weaselbot.combined_aos"]

    base_sql = select(
        ud.c.user_name, u.c.email, ao.c.region_id.label("home_region_id"), func.count().label("attendance_count")
    )
    base_sql = base_sql.select_from(
        u.join(a, u.c.user_id == a.c.user_id)
        .join(b, a.c.beatdown_id == b.c.beatdown_id)
        .join(ao, b.c.ao_id == ao.c.ao_id)
        .join(ud, and_(ud.c.user_id == u.c.user_id, ud.c.region_id == ao.c.region_id))
    )
    base_sql = base_sql.where(func.datediff(func.curdate(), b.c.bd_date) < 30)
    base_sql = base_sql.group_by(ud.c.user_name, u.c.email, ao.c.region_id).cte("z")

    subq = select(
        base_sql,
        func.row_number().over(partition_by=base_sql.c.email, order_by=base_sql.c.attendance_count.desc()).label("rn"),
    ).alias()
    sql = select(subq).where(subq.c.rn == 1)

    dtypes = {
        "user_name": pd.StringDtype(),
        "email": pd.StringDtype(),
        "home_region_id": pd.StringDtype(),
        "attendance_count": pd.Int64Dtype(),
        "rn": pd.Int64Dtype(),
    }

    with engine.begin() as cnxn:
        logging.info("Retrieving home region information...")
        df = pd.read_sql(sql, cnxn, dtype=dtypes)
        logging.info("Done")

    return df


def pull_users(row: tuple[Any, ...], engine: Engine, metadata: MetaData) -> pd.DataFrame:
    dtypes = {
        "slack_user_id": pd.StringDtype(),
        "user_name": pd.StringDtype(),
        "email": pd.StringDtype(),
        "region_id": pd.StringDtype(),
    }
    try:
        usr = Table("users", metadata, autoload_with=engine, schema=row.schema_name)
    except Exception as e:
        logging.error(f"{e}")
        return pd.DataFrame(columns=dtypes.keys())

    sql = select(
        usr.c.user_id.label("slack_user_id"),
        usr.c.user_name,
        usr.c.email,
        literal_column(f"'{row.region_id}'").label("region_id"),
    )

    with engine.begin() as cnxn:
        logging.debug(f"Retrieving user data from {usr.schema}.{usr.name}")
        df = pd.read_sql(sql, cnxn, dtype=dtypes)
        logging.debug("Done")

    return df


def pull_aos(row: tuple[Any, ...], engine: Engine, metadata: MetaData) -> pd.DataFrame:
    dtypes = {"slack_channel_id": pd.StringDtype(), "ao_name": pd.StringDtype(), "region_id": pd.StringDtype()}
    try:
        ao = Table("aos", metadata, autoload_with=engine, schema=row.schema_name)
    except Exception as e:
        logging.error(e)
        return pd.DataFrame(columns=dtypes.keys())

    sql = select(
        ao.c.channel_id.label("slack_channel_id"),
        ao.c.ao.label("ao_name"),
        literal_column(f"'{row.region_id}'").label("region_id"),
    )
    with engine.begin() as cnxn:
        logging.debug(f"Retrieving ao data from {ao.schema}.{ao.name}")
        df = pd.read_sql(sql, cnxn, dtype=dtypes)
        logging.debug("Done")

    return df


def pull_beatdowns(row: tuple[Any, ...], engine: Engine, metadata: MetaData) -> pd.DataFrame:
    dtypes = {
        "slack_channel_id": pd.StringDtype(),
        "slack_q_user_id": pd.StringDtype(),
        "slack_coq_user_id": pd.StringDtype(),
        "pax_count": pd.Int16Dtype(),
        "fng_count": pd.Int16Dtype(),
        "region_id": pd.StringDtype(),
        "timestamp": pd.StringDtype(),
        "ts_edited": pd.StringDtype(),
        "backblast": pd.StringDtype(),
        "json": pd.StringDtype(),
    }
    try:
        beatdowns = Table("beatdowns", metadata, autoload_with=engine, schema=row.schema_name)
    except Exception as e:
        logging.error(e)
        return pd.DataFrame(columns=dtypes.keys())
    try:
        cr = metadata.tables["weaselbot.combined_regions"]
    except KeyError:
        cr = Table("combined_regions", metadata, autoload_with=engine, schema="weaselbot")
    try:
        ao = metadata.tables["weaselbot.combined_aos"]
    except KeyError:
        ao = Table("combined_aos", metadata, autoload_with=engine, schema="weaselbot")
    try:
        cb = metadata.tables["weaselbot.combined_beatdowns"]
    except KeyError:
        cb = Table("combined_beatdowns", metadata, autoload_with=engine, schema="weaselbot")
    try:
        cud = metadata.tables["weaselbot.combined_users_dup"]
    except KeyError:
        cud = Table("combined_users_dup", metadata, autoload_with=engine, schema="weaselbot")

    cte = select(ao.c.slack_channel_id, cb.c.bd_date, cud.c.slack_user_id, cr.c.schema_name)
    cte = cte.select_from(ao.join(cb, ao.c.ao_id == cb.c.ao_id)
                          .join(cud, cb.c.q_user_id == cud.c.user_id)
                          .join(cr, cr.c.region_id == cud.c.region_id))
    cte = cte.where(cud.c.slack_user_id != None).cte()


    sql = select(
        beatdowns.c.ao_id.label("slack_channel_id"),
        beatdowns.c.bd_date,
        beatdowns.c.q_user_id.label("slack_q_user_id"),
        beatdowns.c.coq_user_id.label("slack_coq_user_id"),
        beatdowns.c.pax_count,
        beatdowns.c.fng_count,
        literal_column(f"'{row.region_id}'").label("region_id"),
        beatdowns.c.timestamp,
        beatdowns.c.ts_edited,
        beatdowns.c.backblast,
        beatdowns.c.json,
    )
    sql = sql.outerjoin(cte, and_(cte.c.slack_channel_id == beatdowns.c.ao_id,
                        cte.c.bd_date == beatdowns.c.bd_date,
                        cte.c.slack_user_id == beatdowns.c.q_user_id,
                        cte.c.schema_name == row.schema_name))
    sql = sql.where(and_(cte.c.slack_user_id == None, beatdowns.c.q_user_id != None))

    with engine.begin() as cnxn:
        logging.debug(f"Retrieving beatdown info from {beatdowns.schema}.{beatdowns.name}")
        df = pd.read_sql(sql, cnxn, dtype=dtypes)
        logging.debug(f"Done with {df.shape[0]} records.")
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce", downcast="float")
    df["json"] = df["json"].str.replace("'", '"')  # converting the string object to proper JSON
    df["bd_date"] = pd.to_datetime(df["bd_date"], format="mixed", errors="coerce")

    return df


def pull_attendance(row: tuple[Any, ...], engine: Engine, metadata: MetaData) -> pd.DataFrame:
    dtypes = {
        "slack_channel_id": pd.StringDtype(),
        "slack_q_user_id": pd.StringDtype(),
        "slack_user_id": pd.StringDtype(),
        "region_id": pd.StringDtype(),
        "json": pd.StringDtype(),
    }

    try:
        cr = metadata.tables["weaselbot.combined_regions"]
    except KeyError:
        cr = Table("combined_regions", metadata, autoload_with=engine, schema="weaselbot")
    try:
        ao = metadata.tables["weaselbot.combined_aos"]
    except KeyError:
        ao = Table("combined_aos", metadata, autoload_with=engine, schema="weaselbot")
    try:
        cb = metadata.tables["weaselbot.combined_beatdowns"]
    except KeyError:
        cb = Table("combined_beatdowns", metadata, autoload_with=engine, schema="weaselbot")
    try:
        cud = metadata.tables["weaselbot.combined_users_dup"]
    except KeyError:
        cud = Table("combined_users_dup", metadata, autoload_with=engine, schema="weaselbot")
    try:
        ca = metadata.tables["weaselbot.combined_attendance"]
    except KeyError:
        ca = Table("combined_attendance", metadata, autoload_with=engine, schema="weaselbot")
    try:
        cu = metadata.tables["weaselbot.combined_users"]
    except KeyError:
        cu = Table("combined_users", metadata, autoload_with=engine, schema="weaselbot")
    try:
        attendance = Table("bd_attendance", metadata, autoload_with=engine, schema=row.schema_name)
    except Exception as e:
        logging.error(e)
        return pd.DataFrame(columns=dtypes.keys())
    
    cte = select(ao.c.slack_channel_id, cb.c.bd_date, cud.c.slack_user_id, cr.c.schema_name)
    cte = cte.select_from(cu.join(ca, cu.c.user_id == ca.c.user_id)
                          .join(cb, ca.c.beatdown_id == cb.c.beatdown_id)
                          .join(ao, ao.c.ao_id == cb.c.ao_id)
                          .join(cr, cr.c.region_id == ao.c.region_id)
                          .join(cud, and_(cud.c.user_id == cu.c.user_id, cud.c.region_id == ao.c.region_id))).cte()

    sql = select(
        attendance.c.ao_id.label("slack_channel_id"),
        attendance.c.date.label("bd_date"),
        attendance.c.q_user_id.label("slack_q_user_id"),
        attendance.c.user_id.label("slack_user_id"),
        literal_column(f"'{row.region_id}'").label("region_id"),
        attendance.c.json,
    )
    sql = sql.outerjoin(cte, and_(cte.c.slack_channel_id == attendance.c.ao_id,
                        cte.c.bd_date == attendance.c.date,
                        cte.c.slack_user_id == attendance.c.q_user_id,
                        cte.c.schema_name == row.schema_name))
    sql = sql.where(and_(cte.c.slack_user_id == None, attendance.c.q_user_id != None))

    with engine.begin() as cnxn:
        logging.debug(f"Retrieving attendance info from {attendance.schema}.{attendance.name}")
        df = pd.read_sql(sql, cnxn, dtype=dtypes)
        logging.debug(f"Done with {df.shape[0]} records")

    df["bd_date"] = pd.to_datetime(df["bd_date"], format="mixed", errors="coerce")

    return df


def build_users(
    df_users_dup: pd.DataFrame,
    df_attendance: pd.DataFrame,
    df_home_region: pd.DataFrame,
    engine: Engine,
    metadata: MetaData,
) -> pd.DataFrame:
    """
    Process the user information from each region. Attendance information is taken into account and
    inserted/updated in each target table accordingly. Returns a pandas DataFrame that updates the
    input dataframe `df_users_dup`

    :param df_users_dup: pandas DataFrame object containing each region's user info
    :type df_users_dup: pandas.DataFrame object
    :param df_attendance: pandas DataFrame object containing each region's attendance information
    :type df_attendance: pandas.DataFrame object
    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: pandas.DataFrame
    :return: updated df_users_dup dataframe
    """

    logging.info("building users...")

    cu = metadata.tables["weaselbot.combined_users"]
    cud = metadata.tables["weaselbot.combined_users_dup"]

    df_users_dup["email"] = df_users_dup["email"].str.lower().replace("none", pd.NA)
    df_users_dup = df_users_dup[df_users_dup["email"].notna()]

    df_user_agg = (
        df_attendance.groupby(["slack_user_id"], as_index=False)["bd_date"].count().rename({"bd_date": "count"}, axis=1)
    )
    df_users = (
        df_users_dup.merge(df_user_agg[["slack_user_id", "count"]], on="slack_user_id", how="left") ### how="inner"????
        .fillna(0) ### why? Wouldn't it be better to leave as na?
        .sort_values(by="count", ascending=False)
        .drop_duplicates(subset=["email"], keep="first")
    )

    # update home region using df_home_region
    df_home_region.rename(columns={"user_name": "user_name_home"}, inplace=True)
    df_users = df_users.merge(df_home_region[["user_name_home", "email", "home_region_id"]], on="email", how="left")
    mask = df_users["home_region_id"].isna()
    df_users.loc[mask, "home_region_id"] = df_users.loc[mask, "region_id"]
    mask = df_users["user_name_home"].isna()
    df_users.loc[~mask, "user_name"] = df_users.loc[~mask, "user_name_home"]
    df_users.loc[mask, "user_name_home"] = df_users.loc[mask, "user_name"]
    df_users["home_region_id"] = pd.to_numeric(df_users["home_region_id"], errors="coerce", downcast="integer")

    #### This new section is to try to prevent uploading thousands of records to the table, taking up
    #### a large amount of time. The idea: pull the existing table as a pandas dataframe. Then, merge them,
    #### keeping only what's different between the two tables.
    with engine.begin() as cnxn:
        combined_users = pd.read_sql(select(cu), cnxn)

    combined_users = combined_users.convert_dtypes()

    cols = ["user_name", "email", "home_region_id"]
    df_users = (
        df_users[cols]
        .merge(combined_users[cols], how="left", on=cols, indicator=True)
        .dropna()
        .loc[lambda x: x._merge == "left_only"]
        .drop("_merge", axis=1)
    )
    #### End new logic to reduce load size

    insert_values = df_users[["user_name", "email", "home_region_id"]].to_dict("records")

    if len(insert_values) > 0:
        update_cols = ("user_name", "email", "home_region_id")
        user_insert_sql = insert_statement(cu, insert_values, update_cols)

        with engine.begin() as cnxn:
            logging.info(f"Inserting {len(insert_values):,} records into {cu.schema}.{cu.name}")
            cnxn.execute(user_insert_sql)
            logging.info("Done")
    else:
        logging.info(f"No values needed to be added or updated to {cu.schema}.{cu.name}")

    dtypes = {
        "user_id": pd.StringDtype(),
        "user_name": pd.StringDtype(),
        "email": pd.StringDtype(),
        "home_region_id": pd.StringDtype(),
    }

    with engine.begin() as cnxn:
        df_users = pd.read_sql(select(cu), cnxn, dtype=dtypes)

    df_users_dup = df_users_dup.merge(df_users[["email", "user_id"]], on="email", how="left")
    df_users_dup["user_id"] = pd.to_numeric(df_users_dup["user_id"], errors="coerce", downcast="integer")
    df_users_dup["region_id"] = pd.to_numeric(df_users_dup["region_id"], errors="coerce", downcast="integer")

    ### This new section is to try to prevent uploading thousands of records to the table, taking up
    ### a large amount of time. The idea: pull the existing table as a pandas dataframe. Then, merge them,
    ### keeping only what's different between the two tables.
    with engine.begin() as cnxn:
        combined_users_dup = pd.read_sql(select(cud), cnxn)

    combined_users_dup = combined_users_dup.convert_dtypes()

    cols = ["slack_user_id", "user_name", "email", "region_id", "user_id"]
    df_users_dup_load = (
        df_users_dup[cols]
        .merge(combined_users_dup[cols], how="left", on=cols, indicator=True)
        .loc[
            lambda x: x._merge == 'left_only'
        ]
        .drop(["_merge"], axis=1)
    )
    ### End new logic to reduce load size

    insert_values = (
        df_users_dup_load[["slack_user_id", "user_name", "email", "region_id", "user_id"]]
        .drop_duplicates()
        .to_dict("records")
    )

    if len(insert_values) > 0:
        update_cols = ("user_name", "email", "region_id", "user_id")
        user_dup_insert_sql = insert_statement(cud, insert_values, update_cols)

        with engine.begin() as cnxn:
            logging.info(f"Inserting {len(insert_values):,} into {cud.schema}.{cud.name}")
            cnxn.execute(user_dup_insert_sql)
            logging.info("Done")

    else:
        logging.info(f"No values needed to be added to {cud.schema}.{cud.name}")

    return df_users_dup


def build_aos(df_aos: pd.DataFrame, engine: Engine, metadata: MetaData) -> pd.DataFrame:
    """
    Returns a pandas DataFrame that reflects an update to the input dataframe after
    table inserts/updates.

    :param df_aos: pandas DataFrame object containing each region's AO information
    :type df_aos: pandas.DataFrame object
    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: pandas.DataFrame
    :return: updated df_aos dataframe
    """
    logging.info("building aos...")
    ca = metadata.tables["weaselbot.combined_aos"]
    insert_values = df_aos[["slack_channel_id", "ao_name", "region_id"]].to_dict("records")

    for d in insert_values:
        try:
            d["region_id"] = int(d["region_id"])
        except TypeError:
            pass

    update_cols = ("ao_name",)
    aos_insert_sql = insert_statement(ca, insert_values, update_cols)

    with engine.begin() as cnxn:
        logging.info(f"Inserting {len(insert_values):,} into {ca.schema}.{ca.name}")
        cnxn.execute(aos_insert_sql)
        logging.info("Done")

    dtypes = {
        "ao_id": pd.StringDtype(),
        "slack_channel_id": pd.StringDtype(),
        "ao_name": pd.StringDtype(),
        "region_id": pd.StringDtype(),
    }

    with engine.begin() as cnxn:
        logging.info(f"Retrieving new ao info from {ca.schema}.{ca.name}")
        df = pd.read_sql(select(ca), cnxn, dtype=dtypes)
        logging.info("Done")

    return df


def extract_user_id(slack_user_id) -> NAType | str:
    """
    Process Slack user ID's. Some of these are
    not just simple user ID's. Clean them up
    to standardize across the process.

    :param slack_user_id: User ID from Slack
    :type slack_user_id: str
    :rtype: str | pandas.NA
    :return: cleaned userid string.
    """

    match isinstance(slack_user_id, type(pd.NA)):
        case True:
            return pd.NA
        case _:
            if slack_user_id.startswith("U"):
                return slack_user_id
            elif "team" in slack_user_id:
                return slack_user_id.split("/team/")[1].split("|")[0]


def build_beatdowns(
    df_beatdowns: pd.DataFrame, df_users_dup: pd.DataFrame, df_aos: pd.DataFrame, engine: Engine, metadata: MetaData
) -> pd.DataFrame:
    """
    Returns an updated beatdowns dataframe after updates/inserts to the weaselbot.combined_beatdowns table.

    :param df_beatdowns: pandas DataFrame object containing each region's beatdown information
    :type df_beatdowns: pandas.DataFrame object
    :param df_users_dup: pandas DataFrame object containing each region's users information
    :type df_users_dup: pandas.DataFrame object
    :param df_aos: pandas DataFrame object containing each region's AO information
    :type df_aos: pandas.DataFrame object
    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: pandas.DataFrame
    :return: updated df_beatdowns dataframe
    """

    logging.info("building beatdowns...")
    df_beatdowns["slack_q_user_id"] = df_beatdowns["slack_q_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    df_beatdowns["slack_coq_user_id"] = (
        df_beatdowns["slack_coq_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    )

    cb = metadata.tables["weaselbot.combined_beatdowns"]

    # find duplicate slack_user_ids on df_users_dup
    # df_users_dup is the only table with the "region_id" field as an int in the database.
    # Cast as a string so all other merges work in pandas
    df_users_dup["region_id"] = df_users_dup["region_id"].astype(pd.StringDtype())
    df_beatdowns = (
        df_beatdowns.merge(
            df_users_dup[["slack_user_id", "user_id", "region_id"]],
            left_on=["slack_q_user_id", "region_id"],
            right_on=["slack_user_id", "region_id"],
            how="left",
        )
        .rename({"user_id": "q_user_id"}, axis=1)
        .merge(
            df_users_dup[["slack_user_id", "user_id", "region_id"]],
            left_on=["slack_coq_user_id", "region_id"],
            right_on=["slack_user_id", "region_id"],
            how="left",
        )
        .rename({"user_id": "coq_user_id"}, axis=1)
        .merge(
            df_aos[["slack_channel_id", "ao_id", "region_id"]],
            on=["slack_channel_id", "region_id"],
            how="left",
        )
    )
    df_beatdowns["fng_count"] = df_beatdowns["fng_count"].fillna(0)

    for col in ("ao_id", "q_user_id", "coq_user_id"):
        df_beatdowns[col] = pd.to_numeric(df_beatdowns[col], errors="coerce", downcast="integer")

    insert_values = df_beatdowns[(df_beatdowns["ao_id"].notna()) & (df_beatdowns["q_user_id"].notna())][
        [
            "ao_id",
            "bd_date",
            "q_user_id",
            "coq_user_id",
            "pax_count",
            "fng_count",
            "timestamp",
            "ts_edited",
            "backblast",
            "json",
        ]
    ].to_dict("records")

    # below columns are INT in their target table. coerce them so they'll load properly
    # leaving them as strings in the dataframes for later ease in merges/joins
    # NOTE: YHC is unable to test the JSON datatype. Presumbaly, MySQL will want those
    # sent over as proper dictionaries and not string representations of dictionaries.
    # This is the role of `ast.literal_eval`. If that's not the case, then just remove
    # the `if` statement logic to keep them as strings.
    for d in insert_values:
        if d["json"] is not None:
            d["json"] = ast.literal_eval(d["json"])

    update_cols = ("coq_user_id", "pax_count", "fng_count", "timestamp", "ts_edited", "backblast", "json")

    beatdowns_insert_sql = insert_statement(cb, insert_values, update_cols)

    with engine.begin() as cnxn:
        logging.info(f"Upserting {len(insert_values):,} records into {cb.schema}.{cb.name}")
        cnxn.execute(beatdowns_insert_sql)
        logging.info("Done")

    dtypes = {
        "beatdown_id": pd.StringDtype(),
        "ao_id": pd.StringDtype(),
        "q_user_id": pd.StringDtype(),
        "coq_user_id": pd.StringDtype(),
        "pax_count": pd.Int16Dtype(),
        "fng_count": pd.Int16Dtype(),
        "timestamp": pd.Float64Dtype(),
        "ts_edited": pd.Float64Dtype(),
        "backblast": pd.StringDtype(),
        "json": pd.StringDtype(),
    }

    with engine.begin() as cnxn:
        logging.info(f"Pulling updated table {cb.schema}.{cb.name}")
        df_beatdowns = pd.read_sql(select(cb), cnxn, parse_dates={"bd_date": {"errors": "coerce"}}, dtype=dtypes)
        logging.info("Done")
    df_beatdowns.q_user_id = (
        df_beatdowns.q_user_id.astype(pd.Float64Dtype()).astype(pd.Int64Dtype()).astype(pd.StringDtype())
    )
    return df_beatdowns


def build_attendance(
    df_attendance: pd.DataFrame,
    df_users_dup: pd.DataFrame,
    df_aos: pd.DataFrame,
    df_beatdowns: pd.DataFrame,
    engine: Engine,
    metadata: MetaData,
) -> None:
    """
    Returns None. This process usees all the proir updates to users, AOs and beatdowns to update attendance records in the source
    tables.

    :param df_attendance: pandas DataFrame object containing each region's attendance information
    :type df_attendance: pandas.DataFrame object
    :param df_beatdowns: pandas DataFrame object containing each region's beatdown information
    :type df_beatdowns: pandas.DataFrame object
    :param df_users_dup: pandas DataFrame object containing each region's users information
    :type df_users_dup: pandas.DataFrame object
    :param df_aos: pandas DataFrame object containing each region's AO information
    :type df_aos: pandas.DataFrame object
    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: None
    :return: None
    """

    logging.info("building attendance...")
    catt = metadata.tables["weaselbot.combined_attendance"]
    df_attendance["slack_user_id"] = df_attendance["slack_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    df_attendance["slack_q_user_id"] = df_attendance["slack_q_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    df_users_dup["user_id"] = df_users_dup["user_id"].astype(pd.StringDtype())  #### New
    df_attendance = (
        (
            df_attendance.merge(
                df_users_dup[["slack_user_id", "user_id", "region_id"]],
                left_on=["slack_q_user_id", "region_id"],
                right_on=["slack_user_id", "region_id"],
                how="left",
            )
            .rename({"user_id": "q_user_id", "slack_user_id_x": "slack_user_id"}, axis=1)
            .drop("slack_user_id_y", axis=1)
        )
        .merge(
            df_users_dup[["slack_user_id", "user_id", "region_id"]],
            on=["slack_user_id", "region_id"],
            how="left",
        )
        .merge(
            df_aos[["slack_channel_id", "ao_id", "region_id"]],
            on=["slack_channel_id", "region_id"],
            how="left",
        )
        .merge(
            df_beatdowns[["beatdown_id", "bd_date", "q_user_id", "ao_id"]],
            on=["bd_date", "q_user_id", "ao_id"],
            how="left",
        )
    )

    df_attendance.drop_duplicates(subset=["beatdown_id", "user_id"], inplace=True)
    df_attendance = df_attendance[df_attendance["beatdown_id"].notnull()]
    df_attendance = df_attendance[df_attendance["user_id"].notnull()]
    for col in ("beatdown_id", "user_id"):
        df_attendance[col] = pd.to_numeric(df_attendance[col], errors="coerce", downcast="integer")

    df_attendance = df_attendance.loc[df_attendance["user_id"].notna()]

    insert_values = df_attendance[["beatdown_id", "user_id", "json"]].to_dict("records")

    if len(insert_values) > 0:
        update_cols = ("beatdown_id", "json")
        attendance_insert_sql = insert_statement(catt, insert_values, update_cols)

        with engine.begin() as cnxn:
            logging.info(f"Inserting {len(insert_values):,} values into {catt.schema}.{catt.name}")
            cnxn.execute(attendance_insert_sql)
            logging.info("Done")
    else:
        logging.info("No attendance records to update or insert")


def build_regions(engine: Engine, metadata: MetaData) -> None:
    """Run the regions querie again after all updates are made in order to capture any changes.

    :param engine: SQLAlchemy connection engine to MySQL
    :type engine: sqlalchemy.engine.Engine object
    :param metadata: collection of reflected table metadata
    :type metadata: SQLAlchemy MetaData
    :rtype: None
    :return: None
    """

    cr = metadata.tables["weaselbot.combined_regions"]
    paxminer_region_sql = paxminer_region_query(metadata, cr)
    df_regions = pd.read_sql(paxminer_region_sql, engine)
    insert_values = df_regions[["schema_name", "region_name", "max_timestamp", "max_ts_edited"]].to_dict("records")
    update_cols = ("region_name", "max_timestamp", "max_ts_edited")
    region_insert_sql = insert_statement(cr, insert_values, update_cols)

    with engine.begin() as cnxn:
        logging.info(f"Inserting {len(insert_values):,} values into {cr.schema}.{cr.name}")
        cnxn.execute(region_insert_sql)
        logging.info("Done")


def main() -> None:
    """
    Main function call. This is the process flow for the original code. If not called from the
    command line, then follow this sequence of steps for proper implementation.
    """
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    engine = mysql_connection()
    metadata = MetaData()

    metadata.reflect(engine, schema="weaselbot")
    Table("regions", metadata, autoload_with=engine, schema="paxminer")

    df_regions = region_queries(engine, metadata)
    df_home_region = home_region_query(engine, metadata)

    df_users_dup_list, df_aos_list, df_beatdowns_list, df_attendance_list = [], [], [], []
    for row in df_regions.itertuples(index=False):
        df_users_dup_list.append(pull_users(row, engine, metadata))
        df_aos_list.append(pull_aos(row, engine, metadata))
        df_beatdowns_list.append(pull_beatdowns(row, engine, metadata))
        df_attendance_list.append(pull_attendance(row, engine, metadata))

    df_users_dup = pd.concat([x for x in df_users_dup_list if not x.empty])
    df_aos = pd.concat([x for x in df_aos_list if not x.empty])
    try:
        df_beatdowns = pd.concat([x for x in df_beatdowns_list if not x.empty])
        df_attendance = pd.concat([x for x in df_attendance_list if not x.empty])
    except ValueError:
        logging.info("No new beatdowns to process.")
        sys.exit(0)

    df_beatdowns.ts_edited = pd.to_numeric(df_beatdowns.ts_edited, errors="coerce", downcast="float")

    logging.info(f"beatdowns to process: {len(df_beatdowns)}")
    df_users_dup = build_users(df_users_dup, df_attendance, df_home_region, engine, metadata)
    df_aos = build_aos(df_aos, engine, metadata)
    df_beatdowns = build_beatdowns(df_beatdowns, df_users_dup, df_aos, engine, metadata)
    build_attendance(df_attendance, df_users_dup, df_aos, df_beatdowns, engine, metadata)

    engine.dispose()


if __name__ == "__main__":
    main()
