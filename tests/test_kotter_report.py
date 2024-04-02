import os
from textwrap import TextWrapper

import pandas as pd
import pytest
from sqlalchemy import MetaData

from kotter_report import build_kotter_report, nation_select, region_select, slack_client
from utils import mysql_connection


@pytest.fixture(scope="module")
def connection():
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")
    # Table("regions", metadata, autoload_with=engine, schema="paxminer")

    client = slack_client(os.getenv("SLACK_TOKEN"))
    yield engine, metadata, client

    engine.dispose()


class TestKotterReport:
    def test_nation_select(self, connection):
        engine, metadata, _ = connection
        wrapper = TextWrapper()
        base_sql = [
            "SELECT u.email, a.ao_id, a.ao_name as ao, b.bd_date AS date, YEAR(b.bd_date) AS year_num, ",
            "MONTH(b.bd_date) AS month_num, WEEK(b.bd_date) AS week_num, DAY(b.bd_date) AS day_num, ",
            "CASE WHEN (bd.user_id = b.q_user_id OR bd.user_id = b.coq_user_id) THEN 1 ELSE 0 END AS q_flag  ",
            "FROM weaselbot.combined_attendance AS bd ",
            "INNER JOIN weaselbot.combined_users AS u ",
            "ON u.user_id = bd.user_id ",
            "INNER JOIN weaselbot.combined_beatdowns AS b ",
            "ON bd.beatdown_id = b.beatdown_id ",
            "INNER JOIN weaselbot.combined_aos AS a ",
            "ON b.ao_id = a.ao_id  ",
            "WHERE b.bd_date > 0 ",
            "AND b.bd_date <= CURDATE()",
        ]

        sql = nation_select(metadata)
        raw_sql = sql.compile(engine, compile_kwargs={"literal_binds": True}).__str__()
        assert wrapper.fill("".join(base_sql).lower()) == wrapper.fill(raw_sql).lower()

    def test_region_select(self, connection):
        engine, metadata, _ = connection
        wrapper = TextWrapper()
        base_sql = [
            "SELECT weaselbot.regions.id, weaselbot.regions.paxminer_schema, ",
            "weaselbot.regions.slack_token, weaselbot.regions.send_achievements, ",
            "weaselbot.regions.send_aoq_reports, ",
            "weaselbot.regions.achievement_channel, weaselbot.regions.default_siteq ",
            "FROM weaselbot.regions ",
            "WHERE weaselbot.regions.send_aoq_reports = 1",
        ]
        sql = region_select(metadata)
        raw_sql = sql.compile(engine, compile_kwargs={"literal_binds": True}).__str__()
        assert wrapper.fill("".join(base_sql).lower()) == wrapper.fill(raw_sql).replace("  ", " ").lower()

    def test_build_kotter_report(self):
        df_posts = pd.DataFrame({"pax_id": [1, 2, 3], "col2": ["Me", "Myself", "Irene"]})
        df_qs = pd.DataFrame({"pax_id": [1, 2, 3], "days_since_last_q": [10, pd.NA, 30]})
        report = build_kotter_report(df_posts, df_qs, "Sumo")
        assert "Sumo" in report
        assert "(no Q yet!)" in report
        assert "30 days since last Q" in report
        assert "<@1>" in report
