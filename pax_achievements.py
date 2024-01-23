#!/usr/bin/env /home/epetz/.cache/pypoetry/virtualenvs/weaselbot-7wWSi8jP-py3.11/bin/python3.11

import re
import ssl
import time
from datetime import date
from functools import reduce

import numpy as np
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select, func, case, and_, or_

from f3_data_builder import mysql_connection




def nation_sql(metadata, year):
    u = metadata.tables["weaselbot.combined_users"]
    a = metadata.tables["weaselbot.combined_aos"]
    b = metadata.tables["weaselbot.combined_beatdowns"]
    bd = metadata.tables["weaselbot.combined_attendance"]
    r = metadata.tables["weaselbot.combined_regions"]

    sql = select(
        u.c.email,
        a.c.ao_id,
        a.c.ao_name.label("ao"),
        b.c.bd_date.label("date"),
        func.year(b.c.bd_date).label("year_num"),
        func.month(b.c.bd_date).label("month_num"),
        func.week(b.c.bd_date).label("week_num"),
        func.day(b.c.bd_date).label("day_num"),
        case((or_(bd.c.user_id == b.c.q_user_id, bd.c.user_id == b.c.coq_user_id), 1), else_=0).label("q_flag"),
        b.c.backblast,
        r.c.schema_name.label("home_region"),
    )
    sql = sql.select_from(
        bd.join(u, bd.c.user_id == u.c.user_id)
        .join(b, bd.c.beatdown_id == b.c.beatdown_id)
        .join(a, b.c.ao_id == a.c.ao_id)
        .join(r, u.c.home_region_id == r.c.region_id)
    )
    sql = sql.where(and_(func.year(b.c.bd_date) == year, b.c.bd_date <= func.curdate()))

    return sql


def region_sql(metadata):
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

def main():

    year = date.today().year
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")

    df_regions = pd.read_sql(region_sql(metadata), engine)
    nation_df = pd.read_sql(nation_sql(metadata, year), engine, parse_dates="date")

    for row in df_regions.itertuples(index=False):
        print(f"running {row.paxminer_schema}...")
        awarded_view = award_view(row.paxminer_schema, engine, metadata, year)

        # Import data from SQL
        u = Table("users", metadata, autoload_with=engine, schema=row.paxminer_schema)
        user_table = pd.read_sql(select(u), engine)
        awarded_table_raw = pd.read_sql(awarded_view, engine, parse_dates=["date_awarded"])
        
        if awarded_table_raw.empty:
            continue

        achievement_list = pd.read_sql(select(metadata.tables[f"{row.paxminer_schema}.achievements_list"]), engine)

        df = (
            pd.merge(nation_df, user_table, on="email", how="inner")
            .loc[lambda x: x.home_region == row.paxminer_schema]
            .rename({"user_id": "pax_id", "user_name": "pax"}, axis=1)
        )

        # Create flags for different event types (beatdowns, blackops, qsource, etc)
        df["backblast_title"] = (df["backblast"]
            .fillna("")
            .str.replace("Slackblast: \n", "")
            .str.replace("Backblast:\n", "").str.replace("Backblast!", "") # JS added
            .str.split("\n", expand=True)[0].str.strip()
    )

        df["ruck_flag"] = df.backblast_title.str.lower().str.contains("ruck")
        df.loc[df.ao == "rucking", "ruck_flag"] = True

        df["qsource_flag"] = (
            df["backblast_title"].str.contains(r"q[1-9]\.[1-9]|\b(?:q\s??source)\b", flags=re.IGNORECASE, regex=True))
        
        df.loc[df["ao"] == "qsource", "qsource_flag"] = True

        df["blackops_flag"] = df["backblast_title"].str.contains(r"\b(?:blackops)\b", flags=re.IGNORECASE, regex=True)
        df.loc[df["ao"].isin(("blackops", "csaup", "downrange")), "blackops_flag"] = True

        # Anything that's not a blackops / qsource / ruck is assumed to be a beatdown for counting achievements
        # df[df["ruck_flag"].isna()] # this just prints everything not labeled a ruck.
        df["bd_flag"] = ~df.blackops_flag & ~df.qsource_flag & ~df.ruck_flag

        # Find manually tagged achievements
        df["achievement"] = (
            df["backblast"]
            .str.extract(r"((?<=achievement:).*(?=\n|$))", flags=re.IGNORECASE)[0]
            .str.strip()
            .str.lower()
        )

        # Change q_flag definition to only include qs for beatdowns
        df.q_flag = df.q_flag & df.bd_flag

        # instantiate Slack client
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        slack_client = WebClient(row.slack_token, ssl=ssl_context)

        # Periodic aggregations for automatic achievement tagging - weekly, monthly, yearly
        # "view" tables are aggregated at the level they are calculated, "agg" tables aggregate them to the annual / pax level for joining together

        #####################################
        #           Weekly views            #
        #####################################
        # Beatdowns only, ao / week level
        pax_week_ao_view = (
            df[df.bd_flag]
            .groupby(["week_num", "ao_id", "pax_id"])[["bd_flag", "q_flag"]]
            .sum()
            .rename(columns={"bd_flag": "bd", "q_flag": "q"})
        )
        # Week level
        pax_week_view = pax_week_ao_view.groupby(["week_num", "pax_id"])[["bd", "q"]].agg(["sum", np.count_nonzero])
        pax_week_view.columns = pax_week_view.columns.map("_".join).str.strip("_")
        pax_week_view.rename(
            columns={
                "bd_sum": "bd_sum_week",
                "q_sum": "q_sum_week",
                "bd_count_nonzero": "bd_ao_count_week",
                "q_count_nonzero": "q_ao_count_week",
            },
            inplace=True,
        )
        # Aggregate to pax level
        pax_week_agg = (
            pax_week_view.groupby(["pax_id"])
            .max()
            .rename(
                columns={
                    "bd_sum_week": "bd_sum_week_max",
                    "q_sum_week": "q_sum_week_max",
                    "bd_ao_count_week": "bd_ao_count_week_max",
                    "q_ao_count_week": "q_ao_count_week_max",
                }
            )
        )
        # Special counts for travel bonus
        pax_week_view2 = pax_week_view
        pax_week_view2["bd_ao_count_week_extra"] = pax_week_view2["bd_ao_count_week"] - 1
        pax_week_agg2 = (
            pax_week_view.groupby(["pax_id"])[["bd_ao_count_week_extra"]]
            .sum()
            .rename(columns={"bd_ao_count_week_extra": "bd_ao_count_week_extra_year"})
        )

        # QSources (only once/week counts for points)
        pax_week_other_view = df[df.qsource_flag][["pax_id", "week_num", "qsource_flag"]].drop_duplicates()
        pax_week_other_agg = (
            pax_week_other_view.groupby(["pax_id"])[["qsource_flag"]]
            .count()
            .rename(columns={"qsource_flag": "qsource_week_count"})
        )
        # Count total posts per week (including backops)
        pax_week_other_view2 = df.groupby(["week_num", "pax_id"])[["bd_flag", "blackops_flag"]].sum()
        pax_week_other_view2["bd_blackops_week"] = (
            pax_week_other_view2["bd_flag"] + pax_week_other_view2["blackops_flag"]
        )
        pax_week_other_agg2 = (
            pax_week_other_view2.groupby(["pax_id"])[["bd_blackops_week"]]
            .max()
            .rename(columns={"bd_blackops_week": "bd_blackops_week_max"})
        )

        ######################################
        #           Monthly views            #
        ######################################
        # Beatdowns only , month / ao level
        pax_month_ao_view = (
            df[df.bd_flag]
            .groupby(["month_num", "ao_id", "pax_id"])[["bd_flag", "q_flag"]]
            .sum()
            .rename(columns={"bd_flag": "bd", "q_flag": "q"})
        )
        # Month level
        pax_month_view = pax_month_ao_view.groupby(["month_num", "pax_id"])[["bd", "q"]].agg(["sum", np.count_nonzero])
        pax_month_view.columns = pax_month_view.columns.map("_".join).str.strip("_")
        pax_month_view.rename(
            columns={
                "bd_sum": "bd_sum_month",
                "q_sum": "q_sum_month",
                "bd_count_nonzero": "bd_ao_count_month",
                "q_count_nonzero": "q_ao_count_month",
            },
            inplace=True,
        )
        # Monthly (not just beatdowns, includes QSources and Blackops)
        pax_month_view_other = (
            df.groupby(["month_num", "pax_id"])[["qsource_flag", "blackops_flag"]]
            .sum()
            .rename(columns={"qsource_flag": "qsource_sum_month", "blackops_flag": "blackops_sum_month"})
        )
        # Aggregate to PAX level
        pax_month_agg = (
            pax_month_view.groupby(["pax_id"])
            .max()
            .rename(
                columns={
                    "bd_sum_month": "bd_sum_month_max",
                    "q_sum_month": "q_sum_month_max",
                    "bd_ao_count_month": "bd_ao_count_month_max",
                    "q_ao_count_month": "q_ao_count_month_max",
                }
            )
        )
        pax_month_other_agg = (
            pax_month_view_other.groupby(["pax_id"])
            .max()
            .rename(
                columns={
                    "qsource_sum_month": "qsource_sum_month_max",
                    "blackops_sum_month": "blackops_sum_month_max",
                }
            )
        )
        # Number of unique AOs Q count
        pax_month_q_view = df[df.q_flag].drop_duplicates(["month_num", "pax_id", "ao_id"])
        pax_month_q_view2 = (
            pax_month_q_view.groupby(["month_num", "pax_id"])[["q_flag"]]
            .count()
            .rename(columns={"q_flag": "q_ao_count"})
        )
        pax_month_q_agg = pax_month_q_view2.groupby(["pax_id"]).max().rename(columns={"q_ao_count": "q_ao_month_max"})

        #####################################
        #           Annual views            #
        #####################################
        # Beatdowns only, ao / annual level
        pax_year_ao_view = (
            df[df.bd_flag]
            .groupby(["ao_id", "pax_id"])[["bd_flag", "q_flag"]]
            .sum()
            .rename(columns={"bd_flag": "bd", "q_flag": "q"})
        )
        pax_year_view = pax_year_ao_view.groupby(["pax_id"])[["bd", "q"]].agg(["sum", np.count_nonzero])
        pax_year_view.columns = pax_year_view.columns.map("_".join).str.strip("_")
        pax_year_view.rename(
            columns={
                "bd_sum": "bd_sum_year",
                "q_sum": "q_sum_year",
                "bd_count_nonzero": "bd_ao_count_year",
                "q_count_nonzero": "q_ao_count_year",
            },
            inplace=True,
        )
        # Other than beatdowns
        pax_year_view_other = (
            df.groupby(["pax_id"])[["qsource_flag", "blackops_flag"]]
            .sum()
            .rename(columns={"qsource_flag": "qsource_sum_year", "blackops_flag": "blackops_sum_year"})
        )
        pax_year_ao_view = (
            df[df.bd_flag].groupby(["pax_id", "ao_id"])[["bd_flag"]].count().rename(columns={"bd_flag": "bd_sum_ao"})
        )
        pax_year_ao_agg = (
            pax_year_ao_view.groupby(["pax_id"])[["bd_sum_ao"]].max().rename(columns={"bd_sum_ao": "bd_sum_ao_max"})
        )

        # Merge everything to PAX / annual view
        pax_name_df = df.groupby("pax_id", as_index=False)["pax"].first()
        merge_list = [
            pax_name_df,
            pax_year_view_other,
            pax_year_view,
            pax_year_ao_agg,
            pax_month_other_agg,
            pax_month_q_agg,
            pax_month_agg,
            pax_week_agg,
            pax_week_other_agg,
            pax_week_agg2,
            pax_week_other_agg2,
        ]
        pax_view = reduce(lambda left, right: pd.merge(left, right, on=["pax_id"], how="outer"), merge_list).fillna(0)

        # Calculate automatic achievements
        pax_view["the_priest"] = pax_view["qsource_sum_year"] >= 25
        pax_view["the_monk"] = pax_view["qsource_sum_month_max"] >= 4
        pax_view["leader_of_men"] = pax_view["q_sum_month_max"] >= 4
        pax_view["the_boss"] = pax_view["q_sum_month_max"] >= 6
        pax_view["be_the_hammer_not_the_nail"] = pax_view["q_sum_week_max"] >= 6
        pax_view["cadre"] = pax_view["q_ao_month_max"] >= 7
        pax_view["road_warrior"] = pax_view["bd_ao_count_month_max"] >= 10
        pax_view["el_presidente"] = pax_view["q_sum_year"] >= 20
        pax_view["6_pack"] = pax_view["bd_blackops_week_max"] >= 6
        pax_view["el_quatro"] = pax_view["bd_sum_year"] + pax_view["blackops_sum_year"] >= 25
        pax_view["golden_boy"] = pax_view["bd_sum_year"] + pax_view["blackops_sum_year"] >= 50
        pax_view["centurion"] = pax_view["bd_sum_year"] + pax_view["blackops_sum_year"] >= 100
        pax_view["karate_kid"] = pax_view["bd_sum_year"] + pax_view["blackops_sum_year"] >= 150
        pax_view["crazy_person"] = pax_view["bd_sum_year"] + pax_view["blackops_sum_year"] >= 200
        pax_view["holding_down_the_fort"] = pax_view["bd_sum_ao_max"] >= 50

        # Flag manual acheivements from tagged backblasts
        man_achievement_df = df.loc[~(df.achievement.isna()), ["pax_id", "achievement"]].drop_duplicates(
            ["pax_id", "achievement"]
        )
        man_achievement_df["achieved"] = True
        man_achievement_df = man_achievement_df.pivot(index=["pax_id"], columns=["achievement"], values=["achieved"])

        # Merge to PAX view
        man_achievement_df = man_achievement_df.droplevel(0, axis=1).reset_index()
        pax_view = pd.merge(pax_view, man_achievement_df, on=["pax_id"], how="left")

        # Reshape awarded table and merge
        awarded_table = awarded_table_raw.pivot(index="pax_id", columns="code", values="date_awarded").reset_index()
        awarded_table.set_index("pax_id", inplace=True)
        # awarded_table.columns = [x + '_awarded' for x in awarded_table.columns]
        pax_view = pd.merge(pax_view, awarded_table, how="left", on="pax_id", suffixes=("", "_awarded"))

        # Loop through achievement list, looking for achievements earned but not yet awarded
        award_count = 0
        awards_add = pd.DataFrame(columns=["pax_id", "achievement_id", "date_awarded"])

        for _, row in achievement_list.iterrows():
            award = row["code"]

            # check to see if award has been earned anywhere and / or has been awarded
            if award + "_awarded" in pax_view.columns:
                new_awards = pax_view[(pax_view[award] == True) & (pax_view[award + "_awarded"].isna())]  # noqa: E712
            elif award in pax_view.columns:
                new_awards = pax_view[pax_view[award] == True]  # noqa: E712
            else:
                new_awards = pd.DataFrame()

            if len(new_awards) > 0:
                for pax_row in new_awards.itertuples(index=False):
                    # mark off in the awarded table as awarded for that PAX
                    awards_add.loc[len(awards_add.index)] = [pax_row["pax_id"], row["id"], date.today()]
                    achievements_to_date = len(
                        awarded_table_raw[awarded_table_raw["pax_id"] == pax_row["pax_id"]]
                    ) + len(awards_add[awards_add["pax_id"] == pax_row["pax_id"]])

                    # send to slack channel
                    sMessage = f"Congrats to our man <@{pax_row['pax_id']}>! He just unlocked the achievement *{row['name']}* for {row['verb']}. This is achievement #{achievements_to_date} for <@{pax_row['pax_id']}> this year. Keep up the good work!"
                    print(sMessage)
                    achievement_channel = row.achievement_channel
                    try:
                        response: SlackResponse = slack_client.chat_postMessage(
                            channel=achievement_channel, text=sMessage, link_names=True
                        )
                    except Exception as e:
                        print(f"hit exception {e}")
                        print(e.response)
                        if e.response.get("error") == "not_in_channel":
                            try:
                                print("trying to join channel")
                                slack_client.conversations_join(channel=achievement_channel)
                                response = slack_client.chat_postMessage(
                                    channel=achievement_channel, text=sMessage, link_names=True
                                )
                            except Exception as e:
                                print("hit exception joining channel")
                        elif e.response.get("error") == "ratelimited":
                            try:
                                print("rate limited, sleeping...")
                                time.sleep(10)
                                response = slack_client.chat_postMessage(
                                    channel=achievement_channel, text=sMessage, link_names=True
                                )
                            except Exception:
                                print("hit exception sleeping")
                        else:
                            print("hit unknown exception, unable to send message")

                    try:
                        response2 = slack_client.reactions_add(
                            channel=achievement_channel, name="fire", timestamp=response["ts"]
                        )
                    except Exception:
                        print("could not add reaction")

        # Append new awards to award table
        with engine.connect() as conn:
            awards_add.to_sql(name="achievements_awarded", con=conn, if_exists="append", index=False, schema=row.paxminer_schema)

        # Send confirmation message to paxminer log channel
        ao = Table("aos", metadata, autoload_with=engine, schema=row.paxminer_schema)
        with engine.begin() as cnxn:
            paxminer_log_channel = cnxn.execute(select(ao.c.channel_id).where(ao.c.ao == "paxminer_logs")).scalar()
        try:
            response = slack_client.chat_postMessage(
                channel=paxminer_log_channel,
                text=f"Patch program run for the day, {len(awards_add)} awards tagged",
            )
        except SlackApiError:
            try:
                slack_client.conversations_join(channel=paxminer_log_channel)
                try:
                    response = slack_client.chat_postMessage(
                        channel=paxminer_log_channel,
                        text=f"Patch program run for the day, {len(awards_add)} awards tagged",
                    )
                except Exception:
                    print("hit exception sending message to log channel")
            except Exception:
                print("hit exception joining log channel")
        print(f"All done, {len(awards_add)} awards tagged!")

