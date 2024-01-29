#!/usr/bin/env /home/epetz/.cache/pypoetry/virtualenvs/weaselbot-7wWSi8jP-py3.11/bin/python3.11

from datetime import date

import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import and_, case, func, or_, select

from utils import mysql_connection, slack_client


def nation_sql(metadata, year):
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
    sql = sql.where(b.c.bd_date.between(f"{year}-01-01", func.curdate()),
                    u.c.email != 'none',
                    u.c.user_name != "PAXminer")

    return sql


def region_sql(metadata):
    """Pick out only the regions that want their weaselshaker awards."""
    t = metadata.tables["weaselbot.regions"]
    return select(t).where(t.c.send_achievements == 1)


def award_view(row, engine, metadata, year):
    aa = Table("achievements_awarded", metadata, autoload_with=engine, schema=row.paxminer_schema)
    al = Table("achievements_list", metadata, autoload_with=engine, schema=row.paxminer_schema)

    sql = (
        select(aa, al.c.code)
        .select_from(aa.join(al, aa.c.achievement_id == al.c.id))
        .where(func.year(aa.c.date_awarded) == year)
    )
    return sql

def the_priest(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["slack_user_id", "home_region"]
    x = df.query("@bb_filter or @ao_filter").groupby(grouping)["ao_id"].count()
    return x[x >= 25]

def the_monk(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["month", "slack_user_id", "home_region"]
    x = df.assign(month=df.date.dt.month).query("@bb_filter or @ao_filter").groupby(grouping)["ao_id"].count()
    return x[x>=4]

def leader_of_men(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["month", "slack_user_id", "home_region"]
    x = df.assign(month=df.date.dt.month).query("(q_flag == 1) and not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x[x>=4]

def the_boss(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["month", "slack_user_id", "home_region"]
    x = df.assign(month=df.date.dt.month).query("(q_flag == 1) and not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x[x>=6]

def hammer_not_nail(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["week", "slack_user_id", "home_region"]
    x = df.assign(week=df.date.dt.isocalendar().week).query("(q_flag == 1) and not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x[x>=6]

def cadre(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["month", "slack_user_id", "home_region"]
    x = df.assign(month=df.date.dt.month).query("(q_flag == 1) and not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].nunique()
    return x[x >= 7]

def el_presidente(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["year", "slack_user_id", "home_region"]
    x = df.assign(year=df.date.dt.year).query("(q_flag == 1) and not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x[x >= 20]

def posts(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["year", "slack_user_id", "home_region"]
    x = df.assign(year=df.date.dt.year).query("not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x

def six_pack(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["week", "slack_user_id", "home_region"]
    x = df.assign(week=df.date.dt.isocalendar().week).query("not (@bb_filter or @ao_filter)").groupby(grouping)["ao_id"].count()
    return x[x >= 6]

def hdtf(df: pd.DataFrame, bb_filter: pd.Series, ao_filter: pd.Series) -> pd.Series:
    grouping = ["year", "slack_user_id", "home_region", "ao_id"]
    x = df.assign(year=df.date.dt.year).query("not (@bb_filter or @ao_filter)").groupby(grouping)["ao"].count()
    return x[x >= 50]

def main():

    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")

    df_regions = pd.read_sql(region_sql(metadata), engine)
    nation_df = pd.read_sql(nation_sql(metadata, year), engine, parse_dates="date")

    bb_filter = nation_df.backblast.str.lower().str[:100].str.contains(r"q.{0,1}source|q{0,1}[1-9]\.[0-9]\s|ruck")
    ao_filter = nation_df.ao.str.contains(r"q.{0,1}source|ruck")

    ############# Q Source ##############
    priest_df = the_priest(nation_df, bb_filter, ao_filter)
    monk_df = the_monk(nation_df, bb_filter, ao_filter)
    ############### END #################

    ############ ALL ELSE ###############
    leader_of_men_df = leader_of_men(nation_df, bb_filter, ao_filter)
    boss_df = the_boss(nation_df, bb_filter, ao_filter)
    hammer_not_nail_df = hammer_not_nail(nation_df, bb_filter, ao_filter)
    cadre_df = cadre(nation_df, bb_filter, ao_filter)
    el_presidente_df = el_presidente(nation_df, bb_filter, ao_filter)

    ss = []
    s = posts(nation_df, bb_filter, ao_filter)
    for val in [25, 50, 100, 150, 200]:
        ss.append(s[s >= val])
    el_quatro, golden_by, centurian, karate_kid, crazy_person = ss
    six_pack_df = six_pack(nation_df, bb_filter, ao_filter)
    hdtf_df = hdtf(nation_df, bb_filter, ao_filter)




if __name__ == "__main__":
    main()
