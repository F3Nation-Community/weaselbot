#!/usr/bin/env python

import os
import ast

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, literal_column
from sqlalchemy.sql import select, func, or_
from sqlalchemy.dialects.mysql import insert


def mysql_connection():
    """Connect to MySQL. This involves loading environment variables from file"""
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    dummy = load_dotenv()
    engine = create_engine(
        f"mysql+mysqlconnector://{os.environ.get('DATABASE_USER')}:{os.environ.get('DATABASE_PASSWORD')}@{os.environ.get('DATABASE_HOST')}:3306"
    )
    return engine


def insert_statement(table, insert_values, update_cols):
    sql = insert(table).values(insert_values)
    on_dup = sql.on_duplicate_key_update({v.name: v for v in sql.inserted if v.name in update_cols})
    return on_dup


def region_subquery(metadata):
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


def paxminer_region_query(metadata, cr):
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


def weaselbot_region_query(metadata, cr):
    """
    Construct the region SQL using weaselbot
    """
    sub = region_subquery(metadata)

    sql = select(cr, sub.c.beatdown_count)
    sql = sql.select_from(cr.outerjoin(sub, cr.c.region_id == sub.c.region_id))

    return sql


def region_queries(engine, metadata):
    """
    Using PAXMiner and Weaselbot region tables, make updates to the
    Weaselbot combined_regions table if any exist from PAXMiner.
    """
    cr = metadata.tables["weaselbot.combined_regions"]

    paxminer_region_sql = paxminer_region_query(metadata, cr)

    df_regions = pd.read_sql(paxminer_region_sql, engine)
    insert_values = df_regions.to_dict("records")
    update_cols = ("region_name", "max_timestamp", "max_ts_edited")
    region_insert_sql = insert_statement(cr, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(region_insert_sql)

    dtypes = dict(
        region_id=pd.StringDtype(),
        region_name=pd.StringDtype(),
        schema_name=pd.StringDtype(),
        slack_team_id=pd.StringDtype(),
        max_timestamp=pd.Float64Dtype(),
        max_ts_edited=pd.Float64Dtype(),
        beatdown_count=pd.Int16Dtype(),
    )

    weaselbot_region_sql = weaselbot_region_query(metadata, cr)
    df_regions = pd.read_sql(weaselbot_region_sql, engine, dtype=dtypes)

    return df_regions


def pull_main_data(df_regions, engine, metadata):
    df_users_dup_list, df_aos_list, df_beatdowns_list, df_attendance_list = [], [], [], []

    users_dtypes = dict(
        slack_user_id=pd.StringDtype(), user_name=pd.StringDtype(), email=pd.StringDtype(), region_id=pd.StringDtype()
    )
    ao_dtypes = dict(slack_channel_id=pd.StringDtype(), ao_name=pd.StringDtype(), region_id=pd.StringDtype())
    beatdown_dtypes = dict(
        slack_channel_id=pd.StringDtype(),
        slack_q_user_id=pd.StringDtype(),
        slack_coq_user_id=pd.StringDtype(),
        pax_count=pd.Int16Dtype(),
        fng_count=pd.Int16Dtype(),
        region_id=pd.StringDtype(),
        timestamp=pd.Float64Dtype(),
        ts_edited=pd.StringDtype(),
        backblast=pd.StringDtype(),
        json=pd.StringDtype(),
    )
    attendance_dtypes = dict(
        slack_channel_id=pd.StringDtype(),
        slack_q_user_id=pd.StringDtype(),
        slack_user_id=pd.StringDtype(),
        region_id=pd.StringDtype(),
        json=pd.StringDtype(),
    )

    for row in df_regions.itertuples(index=False):
        print(f"starting {row.schema_name}...", end=" ")
        try:
            db = row.schema_name
            usr = Table("users", metadata, autoload_with=engine, schema=db)
            beatdowns = Table("beatdowns", metadata, autoload_with=engine, schema=db)
            attendance = Table("bd_attendance", metadata, autoload_with=engine, schema=db)
            ao = Table("aos", metadata, autoload_with=engine, schema=db)

            user_sql = select(
                usr.c.user_id.label("slack_user_id"),
                usr.c.user_name,
                usr.c.email,
                literal_column(f"'{row.region_id}'").label("region_id"),
            )

            aos_sql = select(
                ao.c.channel_id.label("slack_channel_id"),
                ao.c.ao.label("ao_name"),
                literal_column(f"'{row.region_id}'").label("region_id"),
            )

            beatdowns_base_sql = select(
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
            beatdowns_sql = beatdowns_base_sql.where(
                or_(beatdowns.c.timestamp > str(row.max_timestamp), beatdowns.c.ts_edited > str(row.max_ts_edited))
            )

            attendance_base_sql = select(
                attendance.c.ao_id.label("slack_channel_id"),
                attendance.c.date.label("bd_date"),
                attendance.c.q_user_id.label("slack_q_user_id"),
                attendance.c.user_id.label("slack_user_id"),
                literal_column(f"'{row.region_id}'").label("region_id"),
                attendance.c.json,
            )
            attendance_sql = attendance_base_sql.where(
                or_(attendance.c.timestamp > str(row.max_timestamp), attendance.c.ts_edited > str(row.max_ts_edited))
            )

            beatdowns_no_ts_sql = beatdowns_base_sql
            attendance_no_ts_sql = attendance_base_sql
            beatdowns_no_ed_sql = beatdowns_base_sql.where(beatdowns.c.timestamp > str(row.max_timestamp))
            attendance_no_ed_sql = attendance_base_sql.where(attendance.c.timestamp > str(row.max_timestamp))

            with engine.begin() as cnxn:
                df_users_dup_list.append(pd.read_sql(user_sql, cnxn, dtype=users_dtypes))
                df_aos_list.append(pd.read_sql(aos_sql, cnxn, dtype=ao_dtypes))
                if (not isinstance(row.max_timestamp, type(pd.NA))) and (not isinstance(row.max_ts_edited, type(pd.NA))):
                    df_beatdowns_list.append(pd.read_sql(beatdowns_sql, cnxn, parse_dates="bd_date", dtype=beatdown_dtypes))
                    df_attendance_list.append(
                        pd.read_sql(attendance_sql, cnxn, parse_dates="bd_date", dtype=attendance_dtypes)
                    )
                elif not isinstance(row.max_timestamp, type(pd.NA)):
                    df_beatdowns_list.append(
                        pd.read_sql(beatdowns_no_ed_sql, cnxn, parse_dates="bd_date", dtype=beatdown_dtypes)
                    )
                    df_attendance_list.append(
                        pd.read_sql(attendance_no_ed_sql, cnxn, parse_dates="bd_date", dtype=attendance_dtypes)
                    )
                elif row.beatdown_count in (pd.NA, 0):
                    df_beatdowns_list.append(
                        pd.read_sql(beatdowns_no_ts_sql, cnxn, parse_dates="bd_date", dtype=beatdown_dtypes)
                    )
                    df_attendance_list.append(
                        pd.read_sql(attendance_no_ts_sql, cnxn, parse_dates="bd_date", dtype=attendance_dtypes)
                    )
        except Exception as e:
            print()
            print(e)
        print("Done")
    df_users_dup = pd.concat(df_users_dup_list)
    df_aos = pd.concat(df_aos_list)
    df_beatdowns = pd.concat(df_beatdowns_list)
    df_attendance = pd.concat(df_attendance_list)

    df_beatdowns.ts_edited = df_beatdowns.ts_edited.replace("NA", pd.NA).astype(pd.Float64Dtype())

    return df_users_dup, df_aos, df_beatdowns, df_attendance


def build_users(df_users_dup, df_attendance, engine, metadata):
    ########## USERS ##########
    print("building users...")

    cu = metadata.tables["weaselbot.combined_users"]
    cud = metadata.tables["weaselbot.combined_users_dup"]

    df_users_dup["email"] = df_users_dup["email"].str.lower()
    df_users_dup = df_users_dup[df_users_dup["email"].notna()]

    df_user_agg = (
        df_attendance.groupby(["slack_user_id"], as_index=False)["bd_date"].count().rename({"bd_date": "count"}, axis=1)
    )
    df_users = (
        df_users_dup.merge(df_user_agg[["slack_user_id", "count"]], on="slack_user_id", how="left")
        .fillna(0)
        .sort_values(by="count", ascending=False)
    )

    df_users.drop_duplicates(subset=["email"], keep="first", inplace=True)

    dtypes = dict(
        user_id=pd.StringDtype(), user_name=pd.StringDtype(), email=pd.StringDtype(), home_region_id=pd.StringDtype()
    )

    insert_values = (
        df_users[["user_name", "email", "region_id"]].rename({"region_id": "home_region_id"}, axis=1).to_dict("records")
    )
    update_cols = ("user_name", "email", "home_region_id")
    user_insert_sql = insert_statement(cu, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(user_insert_sql)

    df_users = pd.read_sql(select(cu), engine, dtype=dtypes)
    df_users_dup = df_users_dup.merge(df_users[["email", "user_id"]], on="email", how="left")

    insert_values = df_users_dup[["slack_user_id", "user_name", "email", "region_id", "user_id"]].to_dict("records")

    for d in insert_values:
        try:
            d["user_id"] = int(d["user_id"])
        except TypeError:
            pass # allowing NA to flow through

    update_cols = ("user_name", "email", "region_id", "user_id")
    user_dup_insert_sql = insert_statement(cud, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(user_dup_insert_sql)

    return df_users_dup


def build_aos(df_aos, engine, metadata):
    ########## AOS ##########
    print("building aos...")
    ca = metadata.tables["weaselbot.combined_aos"]
    insert_values = df_aos[["slack_channel_id", "ao_name", "region_id"]].to_dict("records")
    update_cols = ("ao_name",)
    aos_insert_sql = insert_statement(ca, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(aos_insert_sql)

    dtypes = {
        "ao_id": pd.StringDtype(),
        "slack_channel_id": pd.StringDtype(),
        "ao_name": pd.StringDtype(),
        "region_id": pd.StringDtype(),
    }

    return pd.read_sql(select(ca), engine, dtype=dtypes)


def extract_user_id(slack_user_id):
    match isinstance(slack_user_id, type(pd.NA)):
        case True:
            return pd.NA
        case _:
            if slack_user_id.startswith("U"):
                return slack_user_id
            elif "team" in slack_user_id:
                return slack_user_id.split("/team/")[1].split("|")[0]
            else:
                pass


def build_beatdowns(df_beatdowns, df_users_dup, df_aos, engine, metadata):
    ########## BEATDOWNS ##########
    print("building beatdowns...")
    df_beatdowns["slack_q_user_id"] = df_beatdowns["slack_q_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    df_beatdowns["slack_coq_user_id"] = (
        df_beatdowns["slack_coq_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    )

    cb = metadata.tables["weaselbot.combined_beatdowns"]

    # find duplicate slack_user_ids on df_users_dup
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

    # convert all nans to None
    insert_values = df_beatdowns[df_beatdowns["ao_id"].notna()][
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
    for d in insert_values:
        for col in ("ao_id", "q_user_id", "coq_user_id"):
            try:
                d[col] = int(d[col])
            except TypeError:
                pass
        if d["json"] is not None:
            d["json"] = ast.literal_eval(d["json"])  # on the fence. This may not work

    update_cols = ("coq_user_id", "pax_count", "fng_count", "timestamp", "ts_edited", "backblast", "json")

    beatdowns_insert_sql = insert_statement(cb, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(beatdowns_insert_sql)

    dtypes = dict(
        beatdown_id=pd.StringDtype(),
        ao_id=pd.StringDtype(),
        q_user_id=pd.StringDtype(),
        coq_user_id=pd.StringDtype(),
        pax_count=pd.Int16Dtype(),
        fng_count=pd.Int16Dtype(),
        timestamp=pd.Float64Dtype(),
        ts_edited=pd.Float64Dtype(),
        backblast=pd.StringDtype(),
        json=pd.StringDtype(),
    )

    df_beatdowns = pd.read_sql(select(cb), engine, parse_dates="bd_date", dtype=dtypes)
    df_beatdowns.q_user_id = (
        df_beatdowns.q_user_id.astype(pd.Float64Dtype()).astype(pd.Int64Dtype()).astype(pd.StringDtype())
    )
    return df_beatdowns


def build_attendance(df_attendance, df_users_dup, df_aos, df_beatdowns, engine, metadata):
    ########## ATTENDANCE ##########
    print("building attendance...")
    catt = metadata.tables["weaselbot.combined_attendance"]
    df_attendance["slack_user_id"] = df_attendance["slack_user_id"].apply(extract_user_id).astype(pd.StringDtype())
    df_attendance["slack_q_user_id"] = df_attendance["slack_q_user_id"].apply(extract_user_id).astype(pd.StringDtype())
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

    insert_values = df_attendance[["beatdown_id", "user_id", "json"]].to_dict("records")
    update_cols = ("beatdown_id", "json")
    attendance_insert_sql = insert_statement(catt, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(attendance_insert_sql)


def build_regions(engine, metadata):
    ########## REGIONS ##########

    cr = metadata.tables["weaselbot.combined_regions"]
    paxminer_region_sql = paxminer_region_query(metadata, cr)
    df_regions = pd.read_sql(paxminer_region_sql, engine)
    insert_values = df_regions[["schema_name", "region_name", "max_timestamp", "max_ts_edited"]].to_dict("records")
    update_cols = ("region_name", "max_timestamp", "max_ts_edited")
    region_insert_sql = insert_statement(cr, insert_values, update_cols)

    with engine.begin() as cnxn:
        cnxn.execute(region_insert_sql)


def main():
    engine = mysql_connection()
    metadata = MetaData()

    metadata.reflect(engine, schema="weaselbot")
    Table("regions", metadata, autoload_with=engine, schema="paxminer")

    df_region = region_queries(engine, metadata)
    df_users_dup, df_aos, df_beatdowns, df_attendance = pull_main_data(df_region, engine, metadata)
    print(f"beatdowns to process: {len(df_beatdowns)}")
    df_users_dup = build_users(df_users_dup, df_attendance, engine, metadata)
    df_aos = build_aos(df_aos, engine, metadata)
    df_beatdowns = build_beatdowns(df_beatdowns, df_users_dup, df_aos, engine, metadata)
    build_attendance(df_attendance, df_users_dup, df_aos, df_beatdowns, engine, metadata)

    engine.dispose()


if __name__ == "__main__":
    main()
