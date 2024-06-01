import polars as pl
import pytest
from sqlalchemy import MetaData, create_engine

from kotter_report import nation_sql
from utils import mysql_connection


@pytest.fixture(scope="module")
def engine():
    return mysql_connection()

@pytest.fixture(scope="module")
def metadata():
    return MetaData()

@pytest.fixture(scope="module")
def schemas_df():
    # Replace with your own test data
    data = [("f3chicago",), ("f3_618",), ("f3naperville",), ("f3smokies",)]
    return pl.DataFrame(data, schema=["schema_name"])

def test_nation_sql(schemas_df, engine, metadata):
    expected_sql = (
        "SELECT users.email, users.user_name, users.user_id AS slack_user_id, "
        "bd_attendance.ao_id, aos.ao AS ao, beatdowns.bd_date AS date, "
        "CASE WHEN (bd_attendance.user_id = beatdowns.q_user_id OR bd_attendance.user_id = beatdowns.coq_user_id) THEN 1 ELSE 0 END AS q_flag, "
        "beatdowns.backblast "
        "FROM users JOIN bd_attendance ON bd_attendance.user_id = users.user_id "
        "JOIN beatdowns ON (bd_attendance.q_user_id = beatdowns.q_user_id OR bd_attendance.q_user_id = beatdowns.coq_user_id) "
        "AND bd_attendance.ao_id = beatdowns.ao_id AND bd_attendance.date = beatdowns.bd_date "
        "JOIN aos ON beatdowns.ao_id = aos.channel_id "
        "WHERE YEAR(beatdowns.bd_date) = YEAR(CURDATE()) "
        "AND beatdowns.bd_date <= CURDATE() "
        "AND users.email != 'none' "
        "AND users.user_name != 'PAXminer' "
        "AND beatdowns.q_user_id IS NOT NULL "
        "UNION ALL "
        "SELECT users.email, users.user_name, users.user_id AS slack_user_id, "
        "bd_attendance.ao_id, aos.ao AS ao, beatdowns.bd_date AS date, "
        "CASE WHEN (bd_attendance.user_id = beatdowns.q_user_id OR bd_attendance.user_id = beatdowns.coq_user_id) THEN 1 ELSE 0 END AS q_flag, "
        "beatdowns.backblast "
        "FROM users JOIN bd_attendance ON bd_attendance.user_id = users.user_id "
        "JOIN beatdowns ON (bd_attendance.q_user_id = beatdowns.q_user_id OR bd_attendance.q_user_id = beatdowns.coq_user_id) "
        "AND bd_attendance.ao_id = beatdowns.ao_id AND bd_attendance.date = beatdowns.bd_date "
        "JOIN aos ON beatdowns.ao_id = aos.channel_id "
        "WHERE YEAR(beatdowns.bd_date) = YEAR(CURDATE()) "
        "AND beatdowns.bd_date <= CURDATE() "
        "AND users.email != 'none' "
        "AND users.user_name != 'PAXminer' "
        "AND beatdowns.q_user_id IS NOT NULL"
    )

    query = nation_sql(schemas_df, engine, metadata)
    assert str(query) == expected_sql
