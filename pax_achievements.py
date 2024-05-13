import logging
from datetime import date

import pandas as pd
import polars as pl
from sqlalchemy import MetaData, Selectable, Table, text
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import and_, case, func, literal_column, or_, select, union_all

from utils import mysql_connection, send_to_slack


def home_region_sub_query(u, a, b, ao, date_range):
    """
    Abstract the subquery needed for length of time to 
    look back for considering the home region. This is
    needed because there are many scenarios where a man
    could lapse in attending F3. Many different checks 
    should be considered before defaulting to the maximium
    date range
    """
    subquery = select(u.c.email, func.count(a.c.user_id).label("attendance"))
    subquery = subquery.select_from(u.join(a, a.c.user_id == u.c.user_id)
                          .join(b, and_(a.c.q_user_id == b.c.q_user_id,
                                        a.c.ao_id == b.c.ao_id,
                                        a.c.date == b.c.bd_date))
                          .join(ao, b.c.ao_id == ao.c.channel_id)
                         )
    subquery = subquery.where(func.datediff(func.curdate(), b.c.bd_date) < date_range)
    subquery = subquery.group_by(u.c.email).subquery()
    return subquery

def build_home_regions(schemas, metadata, engine):
    queries = []
    for row in schemas:
        # some men don't post in the immediate prior 30 days. To account for this, 
        # a full year look back is performed. If there are 0 posts in the immediately prior
        # 30 days, the lookback is used. This is a nieve approach and there are
        # edge cases that need to be accounted for.
        # PAX posts a lot in Region 1, moves to Region 2. After 30 days, region 1 stats become
        # null so full year look back is used. Region 1 overrides Region 2 until
        # total beatdowns in Region 2 > total beatdowns in Region 1 for the same year.
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            s1, s2, s3, s4 = (home_region_sub_query(u, a, b, ao, date_range) for date_range in (30, 60, 90, 120))

            sql = select(literal_column(f"'{schema}'").label("region"), 
                        u.c.email,
                        case((s1.c.attendance != None, s1.c.attendance),
                            (s2.c.attendance != None, s2.c.attendance),
                            (s3.c.attendance != None, s3.c.attendance),
                            (s4.c.attendance != None, s4.c.attendance),
                            else_=func.count(a.c.user_id)).label("attendance"))
            sql = sql.select_from(u.join(a, a.c.user_id == u.c.user_id)
                                .join(b, and_(a.c.q_user_id == b.c.q_user_id,
                                                a.c.ao_id == b.c.ao_id,
                                                a.c.date == b.c.bd_date))
                                .join(ao, b.c.ao_id == ao.c.channel_id)
                                .outerjoin(s1, u.c.email == s1.c.email)
                                .outerjoin(s2, u.c.email == s2.c.email)
                                .outerjoin(s3, u.c.email == s3.c.email)
                                .outerjoin(s4, u.c.email == s4.c.email))
            sql = sql.where(func.year(b.c.bd_date) == func.year(func.curdate()))
            sql = sql.group_by(literal_column(f"'{schema}'").label("region"), u.c.email)
            queries.append(sql)
        except Exception:
            print(f"Schema {schema} error.")

    return union_all(*queries)

def nation_sql(schemas, engine, metadata: MetaData) -> Selectable:
    queries = []
    for row in schemas:
        schema = row[0]
        try:
            u = Table("users", metadata, autoload_with=engine, schema=schema)
            a = Table("bd_attendance", metadata, autoload_with=engine, schema=schema)
            b = Table("beatdowns", metadata, autoload_with=engine, schema=schema)
            ao = Table("aos", metadata, autoload_with=engine, schema=schema)

            sql = select(
                u.c.email,
                u.c.user_name,
                u.c.user_id.label("slack_user_id"),
                a.c.ao_id,
                ao.c.ao.label("ao"),
                b.c.bd_date.label("date"),
                case((or_(a.c.user_id == b.c.q_user_id, a.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag"),
                b.c.backblast
            )
            sql = sql.select_from(u.join(a, a.c.user_id == u.c.user_id)
                                    .join(b, and_(or_(a.c.q_user_id == b.c.q_user_id, a.c.q_user_id == b.c.coq_user_id),
                                                    a.c.ao_id == b.c.ao_id,
                                                    a.c.date == b.c.bd_date))
                                    .join(ao, b.c.ao_id == ao.c.channel_id)
                                    )

            sql = sql.where(
                    func.year(b.c.bd_date) == func.year(func.curdate()),
                    u.c.email != "none",
                    u.c.user_name != "PAXminer",
                    b.c.q_user_id != None,
                )
            queries.append(sql)
        except Exception:
            print(f"Schema {schema} error.")

    return union_all(*queries)


def region_sql(metadata: MetaData):
    """Pick out only the regions that want their weaselshaker awards."""
    t = metadata.tables["weaselbot.regions"]
    return select(t).where(t.c.send_achievements == 1)


def award_view(schema, engine: Engine, metadata: MetaData) -> Selectable:
    aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=schema)
    al = Table("achievements_list", metadata, autoload_with=engine, schema=schema)

    sql = (
        select(aa, al.c.code)
        .select_from(aa.join(al, aa.c.achievement_id == al.c.id))
        .where(func.year(aa.c.date_awarded) == func.year(func.curdate()))
    )
    return sql


def award_list(schema, engine: Engine, metadata: MetaData) -> Selectable:
    try:
        al = metadata.tables[f"{schema}.achievements_list"]
    except KeyError:
        al = Table("achievements_list", metadata, autoload_with=engine, schema=schema)

    return select(al)


def the_priest(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["year", "slack_user_id", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) | (ao_filter))
        .group_by(pl.col(grouping)).agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 25)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_monk(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["month", "slack_user_id", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((bb_filter) | (ao_filter))
        .group_by(grouping).agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def leader_of_men(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["month", "slack_user_id", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping).agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 4)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def the_boss(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["month", "slack_user_id", "region"]
    x = (
        df.with_columns(pl.col("date").dt.month().alias("month"))
        .filter((pl.col("q_flag") == 1) & (bb_filter) & (ao_filter))
        .group_by(grouping).agg(pl.col("ao_id").count(), pl.col("date").max())
        .filter(pl.col("ao_id") >= 6)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao_id", "date"])
    )
    return x


def hammer_not_nail(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["week", "slack_user_id", "region"]
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
    grouping = ["month", "slack_user_id", "region"]
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
    grouping = ["year", "slack_user_id", "region"]
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
    grouping = ["year", "slack_user_id", "region"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao_id").count(), pl.col("date").max())
    )
    return x


def six_pack(df: pl.DataFrame, bb_filter: pl.Expr, ao_filter: pl.Expr) -> pl.DataFrame:
    grouping = ["week", "slack_user_id", "region"]
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
    grouping = ["year", "slack_user_id", "region", "ao_id"]
    x = (
        df.with_columns(pl.col("date").dt.year().alias("year"))
        .filter((bb_filter) & (ao_filter))
        .group_by(grouping)
        .agg(pl.col("ao").count(), pl.col("date").max())
        .filter(pl.col("ao") >= 50)
        .with_columns(pl.col("date").alias("date_awarded"))
        .drop(["ao", "date"])
    )
    return x


def load_to_database(row, engine: Engine, metadata: MetaData, data_to_load: pl.DataFrame) -> None:
    """after successfully sending Slack notifications, push the data to the `achievements_awarded` table.
    The data frame data_to_load has already been filtered to include only new achievements."""
    try:
        aa = metadata.tables[f"{row.paxminer_schema}.achievements_awarded"]
    except KeyError:
        aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=row.paxminer_schema)

    load_records = data_to_load.to_dicts()
    sql = insert(aa).values(load_records)
    with engine.begin() as cnxn:
        cnxn.execute(sql)


def main():
    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    t = Table("regions", metadata, autoload_with=engine, schema="paxminer")
    sql = select(t.c.schema_name).where(t.c.schema_name.like("f3%"))
    with engine.begin() as cnxn:
        schemas = cnxn.execute(sql).fetchall()

    home_regions_sql = build_home_regions(schemas, metadata, engine)
    nation_query = nation_sql(schemas, engine, metadata)

    with engine.begin() as cnxn:
        home_regions = pl.from_pandas(pd.read_sql(home_regions_sql, cnxn))
        nation_df = pl.from_pandas(pd.read_sql(nation_query, cnxn))

    nation_df = nation_df.join(home_regions.drop("attendance"), on="email")
    del home_regions

    # for QSource, we want to capture only QSource
    bb_filter = pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s")
    ao_filter = pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source")

    dfs = []
    ############# Q Source ##############
    dfs.append(the_priest(nation_df, bb_filter, ao_filter))
    dfs.append(the_monk(nation_df, bb_filter, ao_filter))
    ############### END #################

    # For beatdowns, we want to exclude QSource and Ruck (blackops too? What is blackops?)
    bb_filter = ~pl.col("backblast").str.slice(0, 100).str.to_lowercase().str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s")
    ao_filter = ~pl.col("ao").str.to_lowercase().str.contains(r"q.{0,1}source|ruck")

    ############ ALL ELSE ###############
    dfs.append(leader_of_men(nation_df, bb_filter, ao_filter))
    dfs.append(the_boss(nation_df, bb_filter, ao_filter))
    dfs.append(hammer_not_nail(nation_df, bb_filter, ao_filter))
    dfs.append(cadre(nation_df, bb_filter, ao_filter))
    dfs.append(el_presidente(nation_df, bb_filter, ao_filter))

    s = posts(nation_df, bb_filter, ao_filter)
    for val in [25, 50, 100, 150, 200]:
        dfs.append(s.filter(pl.col("ao_id") >= val).with_columns(pl.col("date").alias("date_awarded")).drop(["ao_id", "date"]))

    dfs.append(six_pack(nation_df, bb_filter, ao_filter))
    dfs.append(hdtf(nation_df, bb_filter, ao_filter))

    for row in schemas:
        schema = row[0]
        ao = Table("aos", metadata, autoload_with=engine, schema=schema)
        with engine.begin() as cnxn:
            paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
            token = cnxn.execute(select(t.c.slack_token).where(t.c.schema_name == schema)).scalar()
            channel = cnxn.execute(text(f"SELECT achievement_channel FROM weaselbot.regions WHERE paxminer_schema = '{schema}'")).scalar()
        if channel is None:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        try:
            awarded = pl.from_pandas(pd.read_sql(
                award_view(schema, engine, metadata), engine, parse_dates=["date_awarded", "created", "updated"]
            ))
            awards = pl.from_pandas(pd.read_sql(award_list(schema, engine, metadata), engine))
        except NoSuchTableError:
            logging.error(f"{schema} isn't signed up for Weaselbot achievements.")
            continue
        data_to_load = send_to_slack(schema, token, channel, year, awarded, awards, dfs, paxminer_log_channel)
        if not data_to_load.is_empty():
            load_to_database(schema, engine, metadata, data_to_load)

        logging.info(f"Successfully loaded all records and sent all Slack messages for {row.paxminer_schema}.")

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    main()
