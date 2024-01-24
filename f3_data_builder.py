#!/usr/bin/env /home/epetz/.cache/pypoetry/virtualenvs/weaselbot-7wWSi8jP-py3.11/bin/python3.11

import math
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

os.chdir(os.path.dirname(os.path.abspath(__file__)))
dummy = load_dotenv()
engine = create_engine(
    f"mysql+mysqlconnector://{os.environ.get('DATABASE_USER')}:{os.environ.get('DATABASE_PASSWORD')}@{os.environ.get('DATABASE_HOST')}:3306"
)

paxminer_region_sql = """-- sql
SELECT r.schema_name, r.region AS region_name, b.max_timestamp AS max_timestamp, b.max_ts_edited AS max_ts_edited, b.beatdown_count AS beatdown_count, cr.region_id AS region_id
FROM paxminer.regions r
LEFT JOIN weaselbot.combined_regions cr ON r.schema_name = cr.schema_name
LEFT JOIN (SELECT a.region_id, MAX(b.timestamp) AS max_timestamp, MAX(ts_edited) AS max_ts_edited, COUNT(*) AS beatdown_count FROM weaselbot.combined_beatdowns b INNER JOIN weaselbot.combined_aos a ON b.ao_id = a.ao_id GROUP BY a.region_id) b ON cr.region_id = b.region_id
WHERE r.schema_name <> 'f3dc';
"""
weaselbot_region_sql = """-- sql
SELECT w.*, b.beatdown_count AS beatdown_count
FROM weaselbot.combined_regions w
LEFT JOIN (SELECT a.region_id, MAX(b.timestamp) AS max_timestamp, MAX(ts_edited) AS max_ts_edited, COUNT(*) AS beatdown_count FROM weaselbot.combined_beatdowns b INNER JOIN weaselbot.combined_aos a ON b.ao_id = a.ao_id GROUP BY a.region_id) b
ON w.region_id = b.region_id
WHERE w.schema_name <> 'f3dc';
"""
region_insert_sql = "INSERT INTO weaselbot.combined_regions (schema_name, region_name, max_timestamp, max_ts_edited) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE region_name = VALUES(region_name), max_timestamp = VALUES(max_timestamp), max_ts_edited = VALUES(max_ts_edited);"


with engine.connect() as conn:
    df_regions = pd.read_sql(paxminer_region_sql, conn)
    values = df_regions[["schema_name", "region_name", "max_timestamp", "max_ts_edited"]].values.tolist()
    inserted_data = [[None if pd.isnull(value) else value for value in sublist] for sublist in values]
    conn.execute(
        region_insert_sql,
        inserted_data,
    )
    df_regions = pd.read_sql(weaselbot_region_sql, conn)


df_users_dup_list = []
df_aos_list = []
df_beatdowns_list = []
df_attendance_list = []

for _, region_row in df_regions.iterrows():
    db = region_row["schema_name"]
    region_id = region_row["region_id"]
    print(f"pulling {db}...")

    user_sql = f"SELECT user_id AS slack_user_id, user_name, email, {region_id} AS region_id FROM {db}.users;"
    aos_sql = f"SELECT channel_id as slack_channel_id, ao as ao_name, {region_id} AS region_id FROM {db}.aos;"

    beatdowns_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {db}.beatdowns WHERE timestamp > {region_row['max_timestamp']} OR ts_edited > {region_row['max_ts_edited']};"
    attendance_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {region_id} AS region_id, json FROM {db}.bd_attendance WHERE timestamp > {region_row['max_timestamp']} OR ts_edited > {region_row['max_ts_edited']};"
    beatdowns_no_ts_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {db}.beatdowns;"  # AND timestamp > {region_row['max_timestamp'] or 0};"
    attendance_no_ts_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {region_id} AS region_id, json FROM {db}.bd_attendance;"  # AND timestamp > {region_row['max_timestamp'] or 0};"
    beatdowns_no_ed_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {db}.beatdowns WHERE timestamp > {region_row['max_timestamp']};"
    attendance_no_ed_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {region_id} AS region_id, json FROM {db}.bd_attendance WHERE timestamp > {region_row['max_timestamp']};"

    with engine.connect() as conn:
        df_users_dup_list.append(pd.read_sql(user_sql, conn))
        df_aos_list.append(pd.read_sql(aos_sql, conn))
        if (not math.isnan(region_row["max_timestamp"])) and (not math.isnan(region_row["max_ts_edited"])):
            df_beatdowns_list.append(pd.read_sql(beatdowns_sql, conn))
            df_attendance_list.append(pd.read_sql(attendance_sql, conn))
        elif not math.isnan(region_row["max_timestamp"]):
            df_beatdowns_list.append(pd.read_sql(beatdowns_no_ed_sql, conn))
            df_attendance_list.append(pd.read_sql(attendance_no_ed_sql, conn))
        elif region_row["beatdown_count"] == 0:
            df_beatdowns_list.append(pd.read_sql(beatdowns_no_ts_sql, conn))
            df_attendance_list.append(pd.read_sql(attendance_no_ts_sql, conn))

df_users_dup = pd.concat(df_users_dup_list)
df_aos = pd.concat(df_aos_list)
df_beatdowns = pd.concat(df_beatdowns_list)
df_attendance = pd.concat(df_attendance_list)

print(f"beatdowns to process: {len(df_beatdowns)}")

########## USERS ##########
print("building users...")

df_users_dup["email"] = df_users_dup["email"].str.lower()
df_users_dup = df_users_dup[df_users_dup["email"].notna()]

df_user_agg = (
    df_attendance.groupby(["slack_user_id"], as_index=False)["bd_date"].count().rename(columns={"bd_date": "count"})
)
df_users = (
    df_users_dup.merge(df_user_agg[["slack_user_id", "count"]], on="slack_user_id", how="left")
    .fillna(0)
    .sort_values(by="count", ascending=False)
)
df_users.drop_duplicates(subset=["email"], keep="first", inplace=True)
user_insert_sql = "INSERT INTO weaselbot.combined_users (user_name, email, home_region_id) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE user_name = VALUES(user_name), email = VALUES(email), home_region_id = VALUES(home_region_id);"
user_dup_insert_sql = "INSERT INTO weaselbot.combined_users_dup (slack_user_id, user_name, email, region_id, user_id) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_name = VALUES(user_name), email = VALUES(email), region_id = VALUES(region_id), user_id = VALUES(user_id);"

with engine.connect() as conn:
    conn.execute(user_insert_sql, df_users[["user_name", "email", "region_id"]].values.tolist())
    df_users = pd.read_sql_table("combined_users", conn, schema="weaselbot")
    df_users_dup = df_users_dup.merge(df_users[["email", "user_id"]], on="email", how="left").fillna(0)
    df_users_dup["user_id"] = df_users_dup["user_id"].astype(int)
    conn.execute(
        user_dup_insert_sql,
        df_users_dup[["slack_user_id", "user_name", "email", "region_id", "user_id"]].values.tolist(),
    )

########## AOS ##########
print("building aos...")
aos_insert_sql = "INSERT INTO weaselbot.combined_aos (slack_channel_id, ao_name, region_id) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE ao_name = VALUES(ao_name);"

with engine.connect() as conn:
    conn.execute(aos_insert_sql, df_aos[["slack_channel_id", "ao_name", "region_id"]].values.tolist())
    df_aos = pd.read_sql_table("combined_aos", conn, schema="weaselbot")


########## BEATDOWNS ##########
def extract_user_id(slack_user_id):
    if slack_user_id is None:
        return None
    if slack_user_id.startswith("U"):
        return slack_user_id
    else:
        try:
            return slack_user_id.split("/team/")[1].split("|")[0]
        except Exception:
            return None


print("building beatdowns...")
df_beatdowns["slack_q_user_id"] = df_beatdowns["slack_q_user_id"].apply(extract_user_id)
df_beatdowns["slack_coq_user_id"] = df_beatdowns["slack_coq_user_id"].apply(extract_user_id)

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
df_beatdowns["timestamp"] = pd.to_numeric(df_beatdowns["timestamp"], errors="coerce")
df_beatdowns.loc[df_beatdowns["ts_edited"] == "NA", ("ts_edited")] = None
df_beatdowns["ts_edited"] = df_beatdowns["ts_edited"].astype(float)
# df_beatdowns["ao_id"] = df_beatdowns["ao_id"].astype(int)
# df_beatdowns[df_beatdowns["ao_id"].isna()]

# convert all nans to None
values = df_beatdowns[~df_beatdowns["ao_id"].isna()][
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
].values.tolist()
inserted_data = [[None if pd.isnull(value) else value for value in sublist] for sublist in values]

# break up into chunks of 1000
inserted_data = [inserted_data[i : i + 1000] for i in range(0, len(inserted_data), 1000)]

beatdowns_insert_sql = "INSERT INTO weaselbot.combined_beatdowns (ao_id, bd_date, q_user_id, coq_user_id, pax_count, fng_count, timestamp, ts_edited, backblast, json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE coq_user_id = VALUES(coq_user_id), pax_count = VALUES(pax_count), fng_count = VALUES(fng_count), timestamp = VALUES(timestamp), ts_edited = VALUES(ts_edited), backblast = VALUES(backblast), json = VALUES(json);"

with engine.connect() as conn:
    for inserted_data_chunk in inserted_data:
        conn.execute(
            beatdowns_insert_sql,
            inserted_data_chunk,
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
attendance_insert_sql = "INSERT INTO weaselbot.combined_attendance (beatdown_id, user_id, json) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE beatdown_id = VALUES(beatdown_id), json = VALUES(json);"

with engine.connect() as conn:
    conn.execute(attendance_insert_sql, df_attendance[["beatdown_id", "user_id", "json"]].values.tolist())

########## REGIONS ##########
with engine.connect() as conn:
    df_regions = pd.read_sql(paxminer_region_sql, conn)
    values = df_regions[["schema_name", "region_name", "max_timestamp", "max_ts_edited"]].values.tolist()
    inserted_data = [[None if pd.isnull(value) else value for value in sublist] for sublist in values]
    inserted_data = [inserted_data[i : i + 10000] for i in range(0, len(inserted_data), 10000)]
    for inserted_data_chunk in inserted_data:
        conn.execute(
            region_insert_sql,
            inserted_data_chunk,
        )
