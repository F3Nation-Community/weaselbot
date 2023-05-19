###!/mnt/nas/ml/f3-analytics/env/bin/python

from re import T
from textwrap import fill
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
import time
import re
from pandas.core.dtypes.missing import notnull

# from slack import WebClient
import ssl
from slack_sdk import WebClient
from pandas._libs import missing
import os
from dotenv import load_dotenv

# Will need to use PAXMiner creds
dummy = load_dotenv()
DATABASE_USER = os.environ.get("DATABASE_USER")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DATABASE_HOST = os.environ.get("DATABASE_HOST")
engine = create_engine(
    f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:3306"
)

# Inputs
year_select = date.today().year
no_post_threshold = 2
reminder_weeks = 2
home_ao_capture = datetime.combine(
    date.today() + timedelta(weeks=-8), datetime.min.time()
)  # pulls the last 8 weeks to determine home AO
no_q_threshold_weeks = 4
no_q_threshold_posts = 4
active_post_threshold = 3
# db = 'f3stcharles'
# paxminer_log_channel = 'C123'


def build_kotter_report(df_posts: pd.DataFrame, df_qs: pd.DataFrame, siteq: str) -> str:
    # Build Slack message
    sMessage = f"Howdy, <@{siteq}>! This is your weekly WeaselBot Site Q report. According to my records..."

    if len(df_posts) > 0:
        sMessage += "\n\nThe following PAX haven't posted in a bit. \
Now may be a good time to reach out to them when you get a minute. No OYO! :muscle:"

        for index, row in df_posts.iterrows():
            sMessage += "\n- <@" + row["pax_id"] + ">"

    if len(df_qs) > 0:
        sMessage += "\n\nThese guys haven't Q'd anywhere in a while (or at all!):"

        for index, row in df_qs.iterrows():
            sMessage += "\n- <@" + row["pax_id"] + ">"
            if np.isnan(row["days_since_last_q"]):
                sMessage += " (no Q yet!)"
            else:
                sMessage += " (" + str(int(row["days_since_last_q"])) + " days since last Q)"
    return sMessage


# Pull paxminer region data
with engine.connect() as conn:
    df_regions = pd.read_sql_query(
        sql="SELECT * FROM weaselbot.regions WHERE send_aoq_reports = 1;", con=conn
    )

for region_index, region_row in df_regions.iterrows():
    db = region_row["paxminer_schema"]
    slack_secret = region_row["slack_token"]

    print(f"running {db}...")

    # SQL for pull
    sql_select = f"""-- sql
    SELECT bd.user_id AS pax_id,
        u.user_name AS pax,
        bd.ao_id AS ao_id,
        a.ao,
        bd.date,
        YEAR(bd.date) AS year_num,
        MONTH(bd.date) AS month_num,
        WEEK(bd.date) AS week_num,
        DAY(bd.date) AS day_num,
        CASE WHEN bd.user_id = bd.q_user_id OR bd.user_id = b.coq_user_id THEN 1 ELSE 0 END AS q_flag,
        b.backblast
    FROM {db}.bd_attendance bd
    INNER JOIN {db}.users u
    ON bd.user_id = u.user_id
    INNER JOIN {db}.aos a
    ON bd.ao_id = a.channel_id
    INNER JOIN {db}.beatdowns b
    ON bd.ao_id = b.ao_id
        AND bd.date = b.bd_date
        AND bd.q_user_id = b.q_user_id
    WHERE bd.date > 0
    ;
    """

    # df = pd.read_csv('data/master_table.csv', parse_dates=['date'])
    with engine.connect() as conn:
        df = pd.read_sql_query(sql=sql_select, con=conn, parse_dates=["date"])
        df_siteq = pd.read_sql_query(sql=f"SELECT ao, site_q_user_id FROM {db}.aos;", con=conn)
        paxminer_log_channel = conn.execute(
            f"SELECT channel_id FROM {db}.aos WHERE ao = 'paxminer_logs';"
        ).fetchone()[0]

    # Derive home_ao
    home_ao_df = (
        df[df["date"] > home_ao_capture]
        .groupby(["pax_id", "ao"], as_index=False)["day_num"]
        .count()
    )
    # home_ao_df = home_ao_df[home_ao_df['ao'].str.contains('^ao')] # this prevents home AO being assigned to blackops, rucking, etc... could be changed in the future
    home_ao_df.sort_values(["pax_id", "day_num"], ascending=False, inplace=True)
    home_ao_df = home_ao_df.groupby(["pax_id"], as_index=False)["ao"].first()
    home_ao_df.rename(columns={"ao": "home_ao"}, inplace=True)

    # Merge home AO and Site Q
    df = pd.merge(df, home_ao_df, how="left")
    df["home_ao"].fillna("unknown", inplace=True)

    # Group by PAX / week
    df2 = df.groupby(["year_num", "week_num", "pax_id", "home_ao"], as_index=False).agg(
        {"day_num": np.count_nonzero}
    )
    df2.rename(columns={"day_num": "post_count"}, inplace=True)

    # Pull list of weeks
    df3 = df.groupby(["year_num", "week_num"], as_index=False).agg({"date": min})

    # Pull list of PAX
    df4 = df.groupby(["pax_id", "home_ao"], as_index=False)["ao"].count()

    # Cartesian merge
    df5 = pd.merge(df4, df3, how="cross")
    df5.drop(columns=["ao"], axis=1, inplace=True)

    # Join to post counts
    df6 = pd.merge(df5, df2, how="left")
    df6.dropna(subset=["date"], inplace=True)
    df6.fillna(0, inplace=True)

    # Add rolling sums
    df6["post_count_rolling"] = (
        df6.groupby(["pax_id"])["post_count"]
        .rolling(no_post_threshold, min_periods=1)
        .sum()
        .reset_index(drop=True)
    )
    df6["post_count_rolling_stop"] = (
        df6.groupby(["pax_id"])["post_count"]
        .rolling(no_post_threshold + reminder_weeks, min_periods=1)
        .sum()
        .reset_index(drop=True)
    )
    df6["post_count_rolling"] = (
        df6.groupby(["pax_id"])["post_count"]
        .rolling(no_post_threshold, min_periods=1)
        .sum()
        .reset_index(drop=True)
    )

    # Pull pull list of guys not posting
    pull_week = df6[df6["date"] < str(date.today())][
        "date"
    ].max()  # this will only work as expected if you run on Sunday
    # pull_week = datetime(2021, 11, 29, 0, 0, 0)
    df7 = df6[
        (df6["post_count_rolling"] == 0)
        & (df6["date"] == pull_week)
        & (df6["post_count_rolling_stop"] > 0)
    ]

    # Pull pull list of guys not Q-ing
    df8 = (
        df[df["q_flag"] == True]
        .groupby(["pax_id"], as_index=False)["date"]
        .max()
        .rename(columns={"date": "last_q_date"})
    )
    df8["days_since_last_q"] = (datetime.today() - df8["last_q_date"]).dt.days
    df9 = pd.merge(df6, df8, how="left")
    df10 = df9[
        (df9["post_count_rolling"] > 0)
        & (df6["date"] == pull_week)
        & (
            (df9["days_since_last_q"] > (no_q_threshold_weeks * 7))
            | (df9["days_since_last_q"].isna() & (df9["post_count_rolling"] > no_q_threshold_posts))
        )
    ]

    # Merge siteq list
    df_posts = pd.merge(df7, df_siteq, how="left", left_on="home_ao", right_on="ao")
    df_qs = pd.merge(df10, df_siteq, how="left", left_on="home_ao", right_on="ao")
    df_posts = df_posts[
        ~(df_posts["home_ao"] == "unknown")
    ]  # remove NAs... these are guys who haven't posted to a regular AO in the home_ao period
    df_posts.loc[df_posts["site_q_user_id"].isna(), "site_q_user_id"] = region_row["default_siteq"]
    df_qs = df_qs[~(df_qs["home_ao"] == "unknown")]
    df_qs.loc[df_qs["site_q_user_id"].isna(), "site_q_user_id"] = region_row["default_siteq"]

    # instantiate Slack client
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    slack_client = WebClient(slack_secret, ssl=ssl_context)

    # Loop through site-qs that have PAX on the list and send the weaselbot report
    for siteq in df_siteq["site_q_user_id"].unique():  # df8['site_q'].unique():
        dftemp_posts = df_posts[df_posts["site_q_user_id"] == siteq]
        dftemp_qs = df_qs[df_qs["site_q_user_id"] == siteq]

        # Build message
        sMessage = build_kotter_report(dftemp_posts, dftemp_qs, siteq)

        # Send message
        if (len(dftemp_posts) + len(dftemp_qs)) > 0:
            try:
                response = slack_client.chat_postMessage(
                    channel=siteq, text=sMessage, link_names=True
                )
                print(f"Sent {siteq} this message:\n\n{sMessage}\n\n")
            except Exception as e:
                print(f"Error sending message to {siteq}: {e}")

    sMessage = build_kotter_report(df_posts, df_qs, region_row["default_siteq"])
    sMessage += "\n\nNote: If you have listed your site Qs on your aos table, this information will have gone out to them as well."
    try:
        response = slack_client.chat_postMessage(
            channel=region_row["default_siteq"], text=sMessage, link_names=True
        )
        print(f'Sent {region_row["default_siteq"]} this message:\n\n{sMessage}\n\n')
    except Exception as e:
        print(f"hit exception {e}")
        print(e.response)
        if e.response.get("error") == "not_in_channel":
            try:
                print("trying to join channel")
                slack_client.conversations_join(channel=region_row["default_siteq"])
                response = slack_client.chat_postMessage(
                    channel=region_row["default_siteq"], text=sMessage, link_names=True
                )
                print("sent this message:\n\n{sMessage}\n\n")
            except Exception as e:
                print("hit exception joining channel")

    # Send myself a message
    separator = ", "
    try:
        response2 = slack_client.chat_postMessage(
            channel=paxminer_log_channel, text="Successfully sent kotter reports"
        )
        print(f"Sent {paxminer_log_channel} this message:\n\nSuccessfully sent kotter reports\n\n")
    except Exception as e:
        print(f"Error sending message to {paxminer_log_channel}: {e}")  # TODO: add self to channel
    print("All done!")
