from collections import namedtuple
import os
from textwrap import TextWrapper

from dotenv import load_dotenv
from sqlalchemy import MetaData, text
import pandas as pd
import pytest

from weaselbot.f3_data_builder import mysql_connection
from weaselbot.kotter_report import (
    slack_client,
    build_kotter_report,
    nation_select,
    region_select,
    add_home_ao,
    send_weaselbot_report,
    notify_yhc
)


@pytest.fixture(scope="module")
def connection():
    engine = mysql_connection()
    metadata = MetaData()
    metadata.reflect(engine, schema="weaselbot")
    # Table("regions", metadata, autoload_with=engine, schema="paxminer")

    load_dotenv()
    client = slack_client(os.getenv("SLACK_TOKEN"))
    yield engine, metadata, client

    engine.dispose()


class TestKotterReport:
    def test_nation_select(self, connection):
        engine, metadata, _ = connection
        wrapper = TextWrapper()
        base_sql = ["SELECT u.email, a.ao_id, a.ao_name as ao, b.bd_date AS date, YEAR(b.bd_date) AS year_num, ",
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
        raw_sql = sql.compile(engine, compile_kwargs={'literal_binds': True}).__str__()
        assert wrapper.fill("".join(base_sql).lower()) == wrapper.fill(raw_sql).lower()

    def test_region_select(self, connection):
        engine, metadata, _ = connection
        wrapper = TextWrapper()
        base_sql = "SELECT * FROM weaselbot.regions WHERE send_aoq_reports = 1;"