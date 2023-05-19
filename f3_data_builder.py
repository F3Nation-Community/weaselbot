import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from dotenv import load_dotenv
import os

dummy = load_dotenv()
engine = create_engine(
    f"mysql+mysqlconnector://{os.environ.get('DATABASE_USER')}:{os.environ.get('DATABASE_PASSWORD')}@{os.environ.get('DATABASE_HOST')}:3306"
)

region_insert_sql = f"INSERT INTO weaselbot.combined_regions (schema_name, region_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE region_name = VALUES(region_name);"

with engine.connect() as conn:
    df_regions = pd.read_sql_table("regions", conn, schema="paxminer")
    df_regions["region_name"] = ""  # TODO: get this from somewhere
    conn.execute(region_insert_sql, df_regions[["schema_name", "region_name"]].values.tolist())
    df_regions = pd.read_sql_table("combined_regions", conn, schema="weaselbot")


df_users_dup_list = []
df_aos_list = []
df_beatdowns_list = []
df_attendance_list = []

for region_index, region_row in df_regions.iterrows():
    db = region_row["schema_name"]
    region_id = region_row["region_id"]
    print(f"pulling {db}...")

    user_sql = f"SELECT user_id AS slack_user_id, user_name, email, {region_id} AS region_id FROM {db}.users;"
    aos_sql = f"SELECT channel_id as slack_channel_id, ao as ao_name, {region_id} AS region_id FROM {db}.aos;"
    # TODO: use timestamp to limit pulls
    beatdowns_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {region_id} AS region_id FROM {db}.beatdowns WHERE bd_date >= '2023-01-01';"
    attendance_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {region_id} AS region_id FROM {db}.bd_attendance WHERE date >= '2023-01-01';"

    with engine.connect() as conn:
        df_users_dup_list.append(pd.read_sql(user_sql, conn))
        df_aos_list.append(pd.read_sql(aos_sql, conn))
        df_beatdowns_list.append(pd.read_sql(beatdowns_sql, conn))
        df_attendance_list.append(pd.read_sql(attendance_sql, conn))

df_users_dup = pd.concat(df_users_dup_list)
df_aos = pd.concat(df_aos_list)
df_beatdowns = pd.concat(df_beatdowns_list)
df_attendance = pd.concat(df_attendance_list)

########## USERS ##########
print("building users...")

df_users_dup["email"] = df_users_dup["email"].str.lower()
df_users_dup = df_users_dup[df_users_dup["email"].notna()]

df_user_agg = (
    df_attendance.groupby(["slack_user_id"], as_index=False)["bd_date"]
    .count()
    .rename(columns={"bd_date": "count"})
)
df_users = (
    df_users_dup.merge(df_user_agg[["slack_user_id", "count"]], on="slack_user_id", how="left")
    .fillna(0)
    .sort_values(by="count", ascending=False)
)
df_users.drop_duplicates(subset=["email"], keep="first", inplace=True)
user_insert_sql = f"INSERT INTO weaselbot.combined_users (user_name, email, home_region_id) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE user_name = VALUES(user_name), email = VALUES(email), home_region_id = VALUES(home_region_id);"
user_dup_insert_sql = f"INSERT INTO weaselbot.combined_users_dup (slack_user_id, user_name, email, region_id, user_id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_name = VALUES(user_name), email = VALUES(email), region_id = VALUES(region_id), user_id = VALUES(user_id);"

with engine.connect() as conn:
    conn.execute(user_insert_sql, df_users[["user_name", "email", "region_id"]].values.tolist())
    df_users = pd.read_sql_table("combined_users", conn, schema="weaselbot")
    df_users_dup = df_users_dup.merge(
        df_users[["email", "user_id"]], on="email", how="left"
    ).fillna(0)
    df_users_dup["user_id"] = df_users_dup["user_id"].astype(int)
    conn.execute(
        user_dup_insert_sql,
        df_users_dup[
            ["slack_user_id", "user_name", "email", "region_id", "user_id"]
        ].values.tolist(),
    )

########## AOS ##########
print("building aos...")
aos_insert_sql = f"INSERT INTO weaselbot.combined_aos (slack_channel_id, ao_name, region_id) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE ao_name = VALUES(ao_name);"

with engine.connect() as conn:
    conn.execute(
        aos_insert_sql, df_aos[["slack_channel_id", "ao_name", "region_id"]].values.tolist()
    )
    df_aos = pd.read_sql_table("combined_aos", conn, schema="weaselbot")


def extract_user_id(slack_user_id):
    if slack_user_id is None:
        return None
    if slack_user_id.startswith("U"):
        return slack_user_id
    else:
        try:
            return slack_user_id.split("/team/")[1].split("|")[0]
        except:
            return None


df_beatdowns_copy = df_beatdowns.copy()
df_beatdowns = df_beatdowns_copy.copy()
########## BEATDOWNS ##########
print("building beatdowns...")
df_beatdowns["slack_q_user_id"] = df_beatdowns["slack_q_user_id"].apply(extract_user_id)
df_beatdowns["slack_coq_user_id"] = df_beatdowns["slack_coq_user_id"].apply(extract_user_id)
import numpy as np

# find duplicate slack_user_ids on df_users_dup
df_beatdowns = (
    df_beatdowns.merge(
        df_users_dup[["slack_user_id", "user_id", "region_id"]],
        left_on=["slack_q_user_id", "region_id"],
        right_on=["slack_user_id", "region_id"],
        how="left",
    )
    .rename(columns={"user_id": "q_user_id"})
    .fillna(np.nan)
    .replace([np.nan], [None])
)
df_beatdowns = (
    df_beatdowns.merge(
        df_users_dup[["slack_user_id", "user_id", "region_id"]],
        left_on=["slack_coq_user_id", "region_id"],
        right_on=["slack_user_id", "region_id"],
        how="left",
    )
    .rename(columns={"user_id": "coq_user_id"})
    .fillna(np.nan)
    .replace([np.nan], [None])
)
df_beatdowns = df_beatdowns.merge(
    df_aos[["slack_channel_id", "ao_id", "region_id"]],
    on=["slack_channel_id", "region_id"],
    how="left",
)
df_beatdowns["fng_count"] = df_beatdowns["fng_count"].fillna(0).astype(int)


# TODO: convert timestamp and ts_edited
beatdowns_insert_sql = f"INSERT INTO weaselbot.combined_beatdowns (ao_id, bd_date, q_user_id, coq_user_id, pax_count, fng_count) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE coq_user_id = VALUES(coq_user_id), pax_count = VALUES(pax_count), fng_count = VALUES(fng_count);"

with engine.connect() as conn:
    conn.execute(
        beatdowns_insert_sql,
        df_beatdowns[
            ["ao_id", "bd_date", "q_user_id", "coq_user_id", "pax_count", "fng_count"]
        ].values.tolist(),
    )
    df_beatdowns = pd.read_sql_table("combined_beatdowns", conn, schema="weaselbot")

########## ATTENDANCE ##########
print("building attendance...")
df_attendance["slack_user_id"] = df_attendance["slack_user_id"].apply(extract_user_id)
df_attendance["slack_q_user_id"] = df_attendance["slack_q_user_id"].apply(extract_user_id)
df_attendance = df_attendance.merge(
    df_users_dup[["slack_user_id", "user_id", "region_id"]],
    left_on=["slack_q_user_id", "region_id"],
    right_on=["slack_user_id", "region_id"],
    how="left",
).rename(columns={"user_id": "q_user_id", "slack_user_id_x": "slack_user_id"})
df_attendance.drop(columns=["slack_user_id_y"], inplace=True)
df_attendance = df_attendance.merge(
    df_users_dup[["slack_user_id", "user_id", "region_id"]],
    on=["slack_user_id", "region_id"],
    how="left",
)
df_attendance = df_attendance.merge(
    df_aos[["slack_channel_id", "ao_id", "region_id"]],
    on=["slack_channel_id", "region_id"],
    how="left",
)
df_beatdowns["bd_date"] = df_beatdowns["bd_date"].dt.date.astype(str)
# df_attendance["q_user_id"] = df_attendance["q_user_id"].astype(int)
df_attendance = df_attendance.merge(
    df_beatdowns[["beatdown_id", "bd_date", "q_user_id", "ao_id"]],
    on=["bd_date", "q_user_id", "ao_id"],
    how="left",
)
df_attendance = df_attendance.fillna(np.nan).replace([np.nan], [None])
df_attendance.drop_duplicates(subset=["beatdown_id", "user_id"], inplace=True)
df_attendance = df_attendance[df_attendance["beatdown_id"].notnull()]
df_attendance = df_attendance[df_attendance["user_id"].notnull()]
attendance_insert_sql = f"INSERT INTO weaselbot.combined_attendance (beatdown_id, user_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE beatdown_id = VALUES(beatdown_id);"

with engine.connect() as conn:
    conn.execute(attendance_insert_sql, df_attendance[["beatdown_id", "user_id"]].values.tolist())
