import polars as pl
import pytest
from sqlalchemy import Column, Date, Integer, MetaData, String, Table
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select

from kotter_report import (
    build_home_regions,
    build_kotter_report,
    home_region_sub_query,
    nation_sql,
    send_weaselbot_report,
    slack_log,
)
from utils import mysql_connection


@pytest.fixture(scope="module")
def metadata():
    return MetaData()


@pytest.fixture(scope="module")
def engine():
    return mysql_connection()


@pytest.fixture(scope="module")
def schemas_df():
    # Replace with your own test data
    data = [("f3chicago",), ("f3_618",), ("f3naperville",), ("f3smokies",)]
    return pl.DataFrame(data, schema=["schema_name"])


@pytest.fixture(scope="module")
def home_regions_df():
    # Replace with your own test data
    data = [
        ("email1", "region1", "user_id1", 10),
        ("email2", "region1", "user_id2", 5),
        ("email3", "region2", "user_id3", 8),
    ]
    return pl.DataFrame(data, schema=["email", "region", "user_id", "attendance"])


@pytest.fixture(scope="module")
def nation_df():
    # Replace with your own test data
    data = [
        ("email1", "ao1", "AO1", "2022-01-01", 1),
        ("email2", "ao2", "AO2", "2022-01-02", 0),
        ("email3", "ao3", "AO3", "2022-01-03", 1),
    ]
    return pl.DataFrame(
        data, schema=["email", "ao_id", "ao", "date", "q_flag"]
    )


@pytest.fixture(scope="module")
def siteq_df():
    # Replace with your own test data
    data = [
        ("ao1", "AO1", "siteq1"),
        ("ao2", "AO2", "siteq2"),
        ("ao3", "AO3", "siteq3"),
    ]
    return pl.DataFrame(data, schema=["home_ao", "ao", "site_q_user_id"])


@pytest.fixture(scope="module")
def df_mia():
    # Replace with your own test data
    data = [
        ("email1", "user_id1", "region1", "2022-01-01"),
        ("email2", "user_id2", "region1", "2022-01-02"),
    ]
    return pl.DataFrame(data, schema=["email", "user_id", "home_ao", "date"])


@pytest.fixture(scope="module")
def df_lowq():
    # Replace with your own test data
    data = [
        ("email1", "user_id1", "region1", "2022-01-01"),
        ("email2", "user_id2", "region1", "2022-01-02"),
    ]
    return pl.DataFrame(data, schema=["email", "user_id", "home_ao", "date"])


@pytest.fixture(scope="module")
def df_noq():
    # Replace with your own test data
    data = [
        ("email1", "user_id1", "region1"),
        ("email2", "user_id2", "region1"),
    ]
    return pl.DataFrame(data, schema=["email", "user_id", "home_ao"])


@pytest.fixture(scope="module")
def client():
    # Replace with your own test implementation of the Slack WebClient
    return None


def test_home_region_sub_query(metadata, engine):
    u = Table("users", metadata, Column("email", String), Column("user_id", String))
    a = Table("bd_attendance", metadata, Column("user_id", String))
    b = Table("beatdowns", metadata, Column("q_user_id", String), Column("ao_id", String), Column("date", Date))
    ao = Table("aos", metadata, Column("channel_id", String))
    date_range = 30

    subquery = home_region_sub_query(u, a, b, ao, date_range)

    expected_sql = (
        "SELECT users.email, count(bd_attendance.user_id) AS attendance "
        "FROM users JOIN bd_attendance ON bd_attendance.user_id = users.user_id "
        "JOIN beatdowns ON bd_attendance.q_user_id = beatdowns.q_user_id "
        "AND bd_attendance.ao_id = beatdowns.ao_id AND bd_attendance.date = beatdowns.date "
        "JOIN aos ON beatdowns.ao_id = aos.channel_id "
        "WHERE datediff(curdate(), beatdowns.date) < 30 "
        "GROUP BY users.email"
    )
    assert str(subquery) == expected_sql


def test_build_home_regions(schemas_df, metadata, engine, home_regions_df):
    uri = engine.url.render_as_string(hide_password=False).replace("+mysqlconnector", "")
    home_regions_sql = str(build_home_regions(schemas_df, metadata, engine).compile(engine, compile_kwargs={"literal_binds": True}))
    home_regions = pl.read_database_uri(home_regions_sql, uri=uri)
    assert home_regions.frame_equal(home_regions_df)


def test_nation_sql(schemas_df, engine, metadata, nation_df):
    nation_query = nation_sql(schemas_df, engine, metadata)
    assert str(nation_query) == str(nation_df)


def test_build_kotter_report(df_mia, df_lowq, df_noq):
    siteq = "siteq1"
    expected_message = (
        "Howdy, @siteq1! This is your weekly WeaselBot Site Q report. According to my records...\n\n"
        "\nThe following PAX haven't posted in a bit. Now may be a good time to reach out to them when you get a minute. No OYO! :muscle:"
        "\n- <@email1> last posted 2022-01-01"
        "\n- <@email2> last posted 2022-01-02"
        "\n\nThese guys haven't Q'd anywhere in a while (or at all!):"
        "\n- <@email1> hasn't been Q since 2022-01-01. That's 0 days!"
        "\n- <@email2> hasn't been Q since 2022-01-02. That's 0 days!"
        "\n\nNote: If you have listed your site Qs on your aos table, this information will have gone out to them as well."
    )

    message = build_kotter_report(df_mia, df_lowq, df_noq, siteq)
    assert message == expected_message


def test_send_weaselbot_report(client, siteq_df, df_mia, df_lowq, df_noq):
    schema = "schema1"
    default_siteq = "siteq1"

    with pytest.raises(Exception):
        send_weaselbot_report(schema, client, siteq_df, df_mia, df_lowq, df_noq, default_siteq)


def test_slack_log(client):
    schema = "schema1"
    engine = Engine()
    metadata = MetaData()

    with pytest.raises(Exception):
        slack_log(schema, engine, metadata, client)
