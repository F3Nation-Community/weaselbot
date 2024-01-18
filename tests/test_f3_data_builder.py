from decimal import Decimal
from collections import namedtuple

from weaselbot.f3_data_builder import (
    mysql_connection,
    region_subquery,
    paxminer_region_query,
    weaselbot_region_query,
    pull_attendance,
    pull_aos,
    pull_beatdowns,
    pull_users,
    insert_statement,
    build_aos,
    build_attendance,
    build_beatdowns,
    build_regions,
    build_users
)
from sqlalchemy import MetaData, text
import pandas as pd
import pytest


@pytest.fixture(scope="module")
def connection():
    engine = mysql_connection()
    metadata = MetaData()

    metadata.reflect(engine, schema="weaselbot")
    # Table("regions", metadata, autoload_with=engine, schema="paxminer")
    yield engine, metadata

    engine.dispose()


class TestDataBuilder:
    def test_insert(self, connection):
        engine, metadata = connection
        t = metadata.tables['weaselbot.combined_aos']
        insert_values = [
            dict(slack_channel_id="abc", ao_name="ao_myao1", region_id=1),
            dict(slack_channel_id="def", ao_name="ao_myao2", region_id=2),
        ]
        update_cols = ("region_id",)
        sql = insert_statement(t, insert_values, update_cols)
        sql_str = sql.compile(engine, compile_kwargs={'literal_binds': True}).__str__()
        raw_sql = "INSERT INTO weaselbot.combined_aos (slack_channel_id, ao_name, region_id) VALUES ('abc', 'ao_myao1', 1), ('def', 'ao_myao2', 2) AS new ON DUPLICATE KEY UPDATE region_id = new.region_id"

        assert all(x in sql_str for x in ["ao_myao1", "'ao_myao2'", "2", "AS new"])
        assert raw_sql == sql_str

    def test_region_subquery(self, connection):
        engine, metadata = connection
        sql = region_subquery(metadata)
        sql_str = sql.compile(engine, compile_kwargs={'literal_binds': True}).__str__()

        assert sql_str[:6] == "SELECT"
        assert all(x in sql_str for x in ["region_id", "timestamp", "combined_beatdowns", "ao_id", "AS b"])

    def test_paxminer_region_query(self, connection):
        pass

        def test_weaselbot_region_query(self, connection):
            engine, metadata = connection
            cr = metadata.tables["weaselbot.combined_regions"]
            sql = weaselbot_region_query(metadata, cr)
            base_weaselbot_region_sql = """
    SELECT w.*, b.beatdown_count AS beatdown_count
    FROM weaselbot.combined_regions w
    LEFT JOIN (SELECT a.region_id, MAX(b.timestamp) AS max_timestamp, MAX(ts_edited) AS max_ts_edited, COUNT(*) AS beatdown_count FROM weaselbot.combined_beatdowns b INNER JOIN weaselbot.combined_aos a ON b.ao_id = a.ao_id GROUP BY a.region_id) b
    ON w.region_id = b.region_id;
    """

            with engine.begin() as cnxn:
                query1 = cnxn.execute(sql)
                query2 = cnxn.execute(text(base_weaselbot_region_sql))
                data1, data2 = query1.fetchall(), query2.fetchall()
                keys1, keys2 = query1.keys(), query2.keys()

            for record1, record2 in zip(data1, data2):
                for v1, v2 in zip(record1, record2):
                    if type(v1) in (Decimal, float):
                        assert v1 - Decimal(v2) < 1e9
                    else:
                        assert v1 == v2

            assert keys1 == keys2
            assert len(data1) == len(data2)

    def test_pull_users(self, connection):
        engine, metadata = connection
        Row = namedtuple("Row", ["region_id", "schema_name"])
        row = Row(18, "f3chicago")
        df = pull_users(row, engine, metadata)
        base_sql = f"SELECT user_id AS slack_user_id, user_name, email, '{row.region_id}' AS region_id FROM {row.schema_name}.users;"

        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).mean().mean() == 1

    def test_pull_aos(self, connection):
        engine, metadata = connection
        Row = namedtuple("Row", ["region_id", "schema_name"])
        row = Row(18, "f3chicago")
        df = pull_aos(row, engine, metadata)
        base_sql = f"SELECT channel_id as slack_channel_id, ao as ao_name, '{row.region_id}' AS region_id FROM {row.schema_name}.aos;"

        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).mean().mean() == 1

    def test_pull_beatdowns(self, connection):
        engine, metadata = connection
        dtypes = dict(
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
        Row = namedtuple("Row", ["region_id", "schema_name", "max_timestamp", "max_ts_edited"])
        row1 = Row(18, "f3chicago", 1671647384.278359, 1671647542)
        row2 = Row(18, "f3chicago", 1671647384.278359, pd.NA)
        row3 = Row(18, "f3chicago", pd.NA, pd.NA)

        # Test Row1
        df = pull_beatdowns(row1, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {row1.region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {row1.schema_name}.beatdowns WHERE timestamp > {row1.max_timestamp} OR ts_edited > {row1.max_ts_edited};"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1

        # Test Row2
        df = pull_beatdowns(row2, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {row2.region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {row2.schema_name}.beatdowns WHERE timestamp > {row2.max_timestamp};"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1

        # Test Row3
        df = pull_beatdowns(row3, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, bd_date, q_user_id as slack_q_user_id, coq_user_id as slack_coq_user_id, pax_count, fng_count, {row3.region_id} AS region_id, timestamp, ts_edited, backblast, json FROM {row3.schema_name}.beatdowns;"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1

    def test_pull_attendance(self, connection):
        engine, metadata = connection
        dtypes = dict(
            slack_channel_id=pd.StringDtype(),
            slack_q_user_id=pd.StringDtype(),
            slack_user_id=pd.StringDtype(),
            region_id=pd.StringDtype(),
            json=pd.StringDtype(),
        )
        Row = namedtuple("Row", ["region_id", "schema_name", "max_timestamp", "max_ts_edited"])
        row1 = Row(18, "f3chicago", 1671647384.278359, 1671647542)
        row2 = Row(18, "f3chicago", 1671647384.278359, pd.NA)
        row3 = Row(18, "f3chicago", pd.NA, pd.NA)

        # Test Row1
        df = pull_attendance(row1, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {row1.region_id} AS region_id, json FROM {row1.schema_name}.bd_attendance WHERE timestamp > {row1.max_timestamp} OR ts_edited > {row1.max_ts_edited};"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1

        # Test Row2
        df = pull_attendance(row2, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {row2.region_id} AS region_id, json FROM {row2.schema_name}.bd_attendance WHERE timestamp > {row2.max_timestamp};"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1

        # Test Row3
        df = pull_attendance(row3, engine, metadata)
        base_sql = f"SELECT ao_id as slack_channel_id, date as bd_date, q_user_id as slack_q_user_id, user_id as slack_user_id, {row3.region_id} AS region_id, json FROM {row3.schema_name}.bd_attendance;"
        with engine.begin() as cnxn:
            df_base = pd.read_sql(text(base_sql), cnxn, dtype=dtypes)

        for c1, c2 in zip(df.columns, df_base.columns):
            assert c1 == c2
        assert df.shape[0] == df_base.shape[0]
        assert df.shape[1] == df_base.shape[1]
        assert (df == df_base).fillna(True).mean().mean() == 1
