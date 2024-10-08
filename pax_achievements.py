#!/usr/bin/env /Users/jamessheldon/Library/Caches/pypoetry/virtualenvs/weaselbot-93dzw48B-py3.12/bin/python

import logging
from datetime import date
from typing import Tuple

import polars as pl
from sqlalchemy import MetaData, Selectable, Subquery, Table, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.sql import and_, case, func, literal_column, or_, select, union_all

from utils import mysql_connection, send_to_slack


def home_region_sub_query(u: Table, a: Table, b: Table, ao: Table, date_range: int) -> Subquery[Tuple[str, int]]:
    """
    Abstract the subquery needed for length of time to look back for considering the home region. This is
    needed because there are many scenarios where a man could lapse in attending F3. Many different checks
    should be considered before defaulting to the maximium date range
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
    schemas = schemas.filter(~pl.col("schema_name").is_in(["f3devcommunity", "f3development", "f3csra", "f3texarcana"]))
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            s1, s2, s3, s4 = (home_region_sub_query(u, a, b, ao, date_range) for date_range in (30, 60, 90, 120))

            sql = select(
                literal_column(f"'{schema}'").label("region"),
                u.c.email,
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
            sql = sql.group_by(literal_column(f"'{schema}'").label("region"), u.c.email)
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def nation_sql(
    schemas: pl.DataFrame, engine: Engine, metadata: MetaData
) -> Selectable[Tuple[str, str, str, str, str, str, int, str]]:
    """
    The main data set. This is what is used to build all achievement information.
    """
    queries = []
    schemas = schemas.filter(~pl.col("schema_name").is_in(["f3devcommunity", "f3development", "f3csra", "f3texarcana"]))
    for row in schemas.iter_rows():
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            sql = select(
                u.c.email,
                u.c.user_name,
                a.c.ao_id,
                ao.c.ao.label("ao"),
                b.c.bd_date.label("date"),
                case((or_(a.c.user_id == b.c.q_user_id, a.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag"),
                b.c.backblast,
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
                func.year(b.c.bd_date) == func.year(func.curdate()),
                b.c.bd_date <= date.today(),
                u.c.email != "none",
                u.c.user_name != "PAXminer",
                b.c.q_user_id != None,
            )
            queries.append(sql)
        except SQLAlchemyError as e:
            logging.error(f"Schema {schema} error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in schema {schema}: {str(e)}")

    return union_all(*queries)


def the_priest(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Post for 25 Qsource lessons"""
    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) | (ao_filter))
        .group_by(pl.col(grouping))
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 25)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_monk(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Post at 4 QSources in a month"""
    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((bb_filter) | (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def leader_of_men(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Q at 4 beatdowns in a month"""
    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_boss(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Q at 6 beatdowns in a month"""
    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def hammer_not_nail(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Q at 6 beatdowns in a week"""
    grouping = ["week", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.week().alias("week"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def cadre(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Q at 7 different AOs in a month"""
    grouping = ["month", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").n_unique(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 7)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def el_presidente(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Q at 20 beatdowns in a year"""
    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 20)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def posts(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Abstraction of 5 different achievements:
    El Quatro: Post at 25 beatdowns in a year
    Golden Boy: Post at 50 beatdowns in a year
    Centurion: Post at 100 beatdowns in a year
    Karate Kid: Post at 150 beatdowns in a year
    Crazy Person: Post at 200 beatdowns in a year"""
    grouping = ["year", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
    )
    return x


def six_pack(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Post at 6 beatdowns in a week"""
    grouping = ["week", "email", "region"]
    x = (
        df.with_columns(pl.col("date").dt.week().alias("week"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def hdtf(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    """Post 50 times at an AO"""
    grouping = ["year", "email", "region", "ao_id"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao").count(), pl.col("date").max())
        .filter(pl.col("ao") >= 50)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao", "date", "ao_id"])
    )
    return x


def load_to_database(schema: str, engine: Engine, metadata: MetaData, data_to_load: pl.DataFrame) -> None:
    """After successfully sending Slack notifications, push the data to the `achievements_awarded` table.
    The data frame data_to_load has already been filtered to include only new achievements."""
    try:
        aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=schema)
    except NoSuchTableError:
        aa = Table("achievement_awarded", metadata, autoload_with=engine, schema=schema)

    load_records = data_to_load.to_dicts()
    sql = insert(aa).values(load_records)
    with engine.begin() as cnxn:
        cnxn.execute(sql)


def main():
    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    uri = engine.url.render_as_string(hide_password=False).replace("+mysqlconnector", "")
    t = Table("regions", metadata, autoload_with=engine, schema="paxminer")
    sql = str(
        select(t.c.schema_name)
        .where(t.c.schema_name.like("f3%"))
        .compile(engine, compile_kwargs={"literal_binds": True})
    )
    schemas = pl.read_database_uri(query=sql, uri=uri)

    home_regions_sql = str(
        build_home_regions(schemas, metadata, engine).compile(engine, compile_kwargs={"literal_binds": True})
    )
    nation_query = str(nation_sql(schemas, engine, metadata).compile(engine, compile_kwargs={"literal_binds": True}))

    logging.info("Building home regions...")
    home_regions = pl.read_database_uri(query=home_regions_sql, uri=uri)
    logging.info("Building national beatdown data...")
    nation_df = pl.read_database_uri(query=nation_query, uri=uri).with_columns(
        pl.col("backblast").cast(pl.String()), pl.col("ao").cast(pl.String())
    )

    home_regions = home_regions.group_by("email").agg(pl.all().sort_by("attendance").last())
    nation_df = nation_df.join(home_regions.drop("attendance"), on="email")
    del home_regions

    # for QSource, we want to capture only QSource
    bb_filter = (
        pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s")
    )
    ao_filter = pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source")
    logging.info("Building national achievements dataframes...")

    dfs = []
    ############# Q Source ##############
    dfs.append(the_priest(nation_df, bb_filter, ao_filter))
    dfs.append(the_monk(nation_df, bb_filter, ao_filter))
    ############### END #################

    # For beatdowns, we want to exclude QSource and Ruck (blackops too? What is blackops?)
    bb_filter = ~pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(
        r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s"
    )
    ao_filter = ~pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source|ruck")

    ############ ALL ELSE ###############
    dfs.append(leader_of_men(nation_df, bb_filter, ao_filter))
    dfs.append(the_boss(nation_df, bb_filter, ao_filter))
    dfs.append(hammer_not_nail(nation_df, bb_filter, ao_filter))
    dfs.append(cadre(nation_df, bb_filter, ao_filter))
    dfs.append(el_presidente(nation_df, bb_filter, ao_filter))

    s = posts(nation_df, bb_filter, ao_filter)
    for val in [25, 50, 100, 150, 200]:
        dfs.append(
            s.filter(pl.col("ao_id") >= val).with_columns(pl.col("date").alias("date_awarded")).drop(["ao_id", "date"])
        )

    dfs.append(six_pack(nation_df, bb_filter, ao_filter))
    dfs.append(hdtf(nation_df, bb_filter, ao_filter))

    logging.info("Parsing region info and sending to Slack...")
    for row in schemas.iter_rows():
        schema = row[0]
        if schema in ("f3devcommunity", "f3development", "f3csra", "f3texarcana"):
            continue
        try:
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            logging.error(f"No AO table found in in {schema}")
            continue

        with engine.begin() as cnxn:
            paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
            token = cnxn.execute(
                text(f"SELECT slack_token FROM weaselbot.regions WHERE paxminer_schema = '{schema}'")
            ).scalar()
            channel = cnxn.execute(
                text(f"SELECT achievement_channel FROM weaselbot.regions WHERE paxminer_schema = '{schema}'")
            ).scalar()
        if channel is None:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        try:
            al = Table("achievements_list", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        try:
            aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=schema)
        except NoSuchTableError:
            aa = Table("achievement_awarded", metadata, autoload_with=engine, schema=schema)

        sql = (
            select(aa, al.c.code)
            .select_from(aa.join(al, aa.c.achievement_id == al.c.id))
            .where(func.year(aa.c.date_awarded) == func.year(func.curdate()))
        )

        awarded = pl.read_database_uri(str(sql.compile(engine, compile_kwargs={"literal_binds": True})), uri=uri)
        awards = pl.read_database_uri(f"SELECT * FROM {schema}.achievements_list", uri=uri)

        # we're pushing one schema at a time to Slack. Ensure all slack_id's are valid for that specific schema
        users = pl.read_database_uri(f"SELECT email, user_id as slack_user_id FROM {schema}.users", uri=uri)
        dfs_regional = []
        for df in dfs:
            dfs_regional.append(df.filter(pl.col("region") == schema).join(users, on="email").drop("email"))

        data_to_load = send_to_slack(schema, token, channel, year, awarded, awards, dfs_regional, paxminer_log_channel)
        if not data_to_load.is_empty():
            load_to_database(schema, engine, metadata, data_to_load)

        logging.info(f"Successfully loaded all records and sent all Slack messages for {schema}.")

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    main()
