import logging
from datetime import date

import pandas as pd
from sqlalchemy import MetaData, Selectable, Table
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import and_, case, func, or_, select

from utils import mysql_connection, send_to_slack


def nation_sql(metadata: MetaData, year: int) -> Selectable:
    u = metadata.tables["weaselbot.combined_users"]
    a = metadata.tables["weaselbot.combined_aos"]
    b = metadata.tables["weaselbot.combined_beatdowns"]
    bd = metadata.tables["weaselbot.combined_attendance"]
    r = metadata.tables["weaselbot.combined_regions"]
    cu = metadata.tables["weaselbot.combined_users_dup"]

    sql = select(
        u.c.email,
        u.c.user_name,
        cu.c.slack_user_id,
        a.c.ao_id,
        a.c.ao_name.label("ao"),
        b.c.bd_date.label("date"),
        case((or_(bd.c.user_id == b.c.q_user_id, bd.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag"),
        b.c.backblast,
        r.c.schema_name.label("home_region"),
    )
    sql = sql.select_from(
        bd.join(u, bd.c.user_id == u.c.user_id)
        .join(cu, and_(u.c.email == cu.c.email, u.c.home_region_id == cu.c.region_id))
        .join(b, bd.c.beatdown_id == b.c.beatdown_id)
        .join(a, b.c.ao_id == a.c.ao_id)
        .join(r, u.c.home_region_id == r.c.region_id)
    )
    sql = sql.where(
        b.c.bd_date.between(f"{year}-01-01", func.curdate()), u.c.email != "none", u.c.user_name != "PAXminer"
    )

    return sql


def region_sql(metadata: MetaData):
    """Pick out only the regions that want their weaselshaker awards."""
    t = metadata.tables["weaselbot.regions"]
    return select(t).where(t.c.send_achievements == 1)


def award_view(row, engine: Engine, metadata: MetaData, year: int) -> Selectable:
    aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=row.paxminer_schema)
    al = Table("achievements_list", metadata, autoload_with=engine, schema=row.paxminer_schema)

    sql = (
        select(aa, al.c.code)
        .select_from(aa.join(al, aa.c.achievement_id == al.c.id))
        .where(func.year(aa.c.date_awarded) == year)
    )
    return sql


def award_list(row, engine: Engine, metadata: MetaData) -> Selectable:
    try:
        al = metadata.tables[f"{row.paxminer_schema}.achievements_list"]
    except KeyError:
        al = Table("achievements_list", metadata, autoload_with=engine, schema=row.paxminer_schema)

    return select(al)


def the_priest(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["year", "slack_user_id", "home_region"]
    x = (
        df.assign(year=df.date.dt.year)
        .query("@bb_filter or @ao_filter")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 25].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def the_monk(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["month", "slack_user_id", "home_region"]
    x = (
        df.assign(month=df.date.dt.month)
        .query("@bb_filter or @ao_filter")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 4].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def leader_of_men(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["month", "slack_user_id", "home_region"]
    x = (
        df.assign(month=df.date.dt.month)
        .query("(q_flag == 1) and not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 4].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def the_boss(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["month", "slack_user_id", "home_region"]
    x = (
        df.assign(month=df.date.dt.month)
        .query("(q_flag == 1) and not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 6].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def hammer_not_nail(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["week", "slack_user_id", "home_region"]
    x = (
        df.assign(week=df.date.dt.isocalendar().week)
        .query("(q_flag == 1) and not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 6].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def cadre(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["month", "slack_user_id", "home_region"]
    x = (
        df.assign(month=df.date.dt.month)
        .query("(q_flag == 1) and not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "nunique", "date": "max"})
    )
    return x[x.ao_id >= 7].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def el_presidente(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["year", "slack_user_id", "home_region"]
    x = (
        df.assign(year=df.date.dt.year)
        .query("(q_flag == 1) and not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 20].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def posts(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["year", "slack_user_id", "home_region"]
    x = (
        df.assign(year=df.date.dt.year)
        .query("not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x


def six_pack(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["week", "slack_user_id", "home_region"]
    x = (
        df.assign(week=df.date.dt.isocalendar().week)
        .query("not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao_id": "count", "date": "max"})
    )
    return x[x.ao_id >= 6].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1)


def hdtf(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.DataFrame:
    grouping = ["year", "slack_user_id", "home_region", "ao_id"]
    x = (
        df.assign(year=df.date.dt.year)
        .query("not (@bb_filter or @ao_filter)")
        .groupby(grouping)
        .agg({"ao": "count", "date": "max"})
    )
    return x[x.ao >= 50].reset_index().drop("ao", axis=1).rename({"date": "date_awarded"}, axis=1)


def load_to_database(row, engine: Engine, metadata: MetaData, data_to_load: pd.DataFrame) -> None:
    """after successfully sending Slack notifications, push the data to the `achievements_awarded` table.
    The data frame data_to_load has already been filtered to include only new achievements."""
    try:
        aa = metadata.tables[f"{row.paxminer_schema}.achievements_awarded"]
    except KeyError:
        aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=row.paxminer_schema)

    load_records = data_to_load.to_dict("records")
    sql = insert(aa).values(load_records)
    with engine.begin() as cnxn:
        cnxn.execute(sql)


def main():
    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")

    df_regions = pd.read_sql(region_sql(metadata), engine)
    nation_df = pd.read_sql(nation_sql(metadata, year), engine, parse_dates="date")

    # for QSource, we want to capture only QSource
    bb_filter = nation_df.backblast.str.lower().str[:100].str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s")
    ao_filter = nation_df.ao.str.contains(r"q.{0,1}source")

    dfs = []
    ############# Q Source ##############
    dfs.append(the_priest(nation_df, bb_filter, ao_filter))
    dfs.append(the_monk(nation_df, bb_filter, ao_filter))
    ############### END #################

    # For beatdowns, we want to exclude QSource and Ruck (blackops too? What is blackops?)
    bb_filter = nation_df.backblast.str.lower().str[:100].str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s|ruck")
    ao_filter = nation_df.ao.str.contains(r"q.{0,1}source|ruck")

    ############ ALL ELSE ###############
    dfs.append(leader_of_men(nation_df, bb_filter, ao_filter))
    dfs.append(the_boss(nation_df, bb_filter, ao_filter))
    dfs.append(hammer_not_nail(nation_df, bb_filter, ao_filter))
    dfs.append(cadre(nation_df, bb_filter, ao_filter))
    dfs.append(el_presidente(nation_df, bb_filter, ao_filter))

    s = posts(nation_df, bb_filter, ao_filter)
    for val in [25, 50, 100, 150, 200]:
        dfs.append(s[s.ao_id >= val].reset_index().drop("ao_id", axis=1).rename({"date": "date_awarded"}, axis=1))

    dfs.append(six_pack(nation_df, bb_filter, ao_filter))
    dfs.append(hdtf(nation_df, bb_filter, ao_filter))

    for row in df_regions.itertuples(index=False):
        try:
            awarded = pd.read_sql(
                award_view(row, engine, metadata, year), engine, parse_dates=["date_awarded", "created", "updated"]
            )
            awards = pd.read_sql(award_list(row, engine, metadata), engine)
        except NoSuchTableError:
            logging.error(f"{row.paxminer_schema} isn't signed up for Weaselbot achievements.")
            continue
        data_to_load = send_to_slack(row, year, awarded, awards, dfs)
        if not data_to_load.empty:
            load_to_database(row, engine, metadata, data_to_load)
        logging.info(f"Successfully loaded all records and sent all Slack messages for {row.paxminer_schema}.")

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )
    main()
