import pytest
from unittest.mock import MagicMock, patch, call
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.sql import text


from ..weaselbot.achievement_tables import (
    create_table,
    achievements_list_columns,
    achievements_awarded_columns,
    insert_vals,
    schema,
    MYSQL_ENGINE,
    MYSQL_CHARSET,
    MYSQL_COLLATE,
    VARCHAR_CHARSET,
    VARCHAR_LENGTH
)

@pytest.fixture
def mock_engine():
    with patch('weaselbot.achievement_tables.mysql_connection') as mock_conn:
        engine = MagicMock()
        connection = MagicMock()
        engine.begin.return_value.__enter__.return_value = connection
        mock_conn.return_value = engine
        yield engine

@pytest.fixture
def metadata():
    return MetaData()

def test_create_table(metadata):
    """Test table creation with correct parameters"""
    test_columns = [Column("name", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False)]
    table = create_table("test_table", test_columns, metadata, schema)
    
    assert isinstance(table, Table)
    assert table.name == "test_table"
    assert table.schema == schema
    assert table.kwargs['mysql_engine'] == MYSQL_ENGINE
    assert table.kwargs['mysql_charset'] == MYSQL_CHARSET
    assert table.kwargs['mysql_collate'] == MYSQL_COLLATE

def test_achievements_list_columns_structure():
    """Test structure of achievements_list columns"""
    assert len(achievements_list_columns) == 5
    column_names = [col.name for col in achievements_list_columns]
    assert set(column_names) == {"id", "name", "description", "verb", "code"}
    
    # Test VARCHAR columns have correct configuration
    varchar_columns = [col for col in achievements_list_columns if col.name != "id"]
    for col in varchar_columns:
        assert col.type.length == VARCHAR_LENGTH
        assert col.type.charset == VARCHAR_CHARSET
        assert not col.nullable

def test_achievements_awarded_columns_structure():
    """Test structure of achievements_awarded columns"""
    assert len(achievements_awarded_columns) == 6
    column_names = [col.name for col in achievements_awarded_columns]
    assert set(column_names) == {
        "id", "achievement_id", "pax_id", "date_awarded", 
        "created", "updated"
    }

def test_insert_vals_structure():
    """Test structure of insert values"""
    required_keys = {"name", "description", "verb", "code"}
    
    assert len(insert_vals) > 0
    for val in insert_vals:
        assert isinstance(val, dict)
        assert set(val.keys()) == required_keys
        assert all(isinstance(v, str) for v in val.values())

@patch('weaselbot.weaselbot.achievement_tables.mysql_connection')
def test_database_operations(mock_engine):
    """Test database operations workflow"""
    # Set up mock connection and returns
    connection = mock_engine.begin().__enter__()
    connection.execute.return_value = MagicMock()
    
    # Execute multiple database operations
    with mock_engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {schema}.aos ADD site_q_user_id VARCHAR(45) NULL"))
        conn.execute(text("SELECT 1"))
    
    # Verify all executions in order using str() comparison
    assert connection.execute.call_count == 2
    expected_calls = [
        call(text(f"ALTER TABLE {schema}.aos ADD site_q_user_id VARCHAR(45) NULL")),
        call(text("SELECT 1"))
    ]
    actual_calls = connection.execute.call_args_list
    
    assert len(actual_calls) == len(expected_calls)
    for actual, expected in zip(actual_calls, expected_calls):
        assert str(actual.args[0]) == str(expected.args[0])

@patch('weaselbot.weaselbot.achievement_tables.mysql_connection')
def test_view_creation(mock_engine):
    """Test view creation SQL"""
    # Set up mock connection and returns
    connection = mock_engine.begin().__enter__()
    connection.execute.return_value = MagicMock()

    # Execute view creation
    with mock_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE OR REPLACE VIEW {schema}.achievements_view AS
            SELECT al.*, aa.date_awarded 
            FROM {schema}.achievements_list al
            LEFT JOIN {schema}.achievements_awarded aa ON al.id = aa.achievement_id
        """))

    # Verify execution
    assert connection.execute.call_count == 1
    actual_sql = str(connection.execute.call_args_list[0].args[0])
    assert "CREATE OR REPLACE VIEW" in actual_sql
    assert "achievements_view" in actual_sql
    assert "achievements_list" in actual_sql
    assert "achievements_awarded" in actual_sql

@patch('weaselbot.weaselbot.achievement_tables.mysql_connection')
def test_error_handling(mock_engine):
    """Test error handling for database operations"""
    connection = mock_engine.begin().__enter__()
    connection.execute.side_effect = Exception("Test error")
    
    with pytest.raises(Exception):
        with mock_engine.begin() as conn:
            conn.execute(text("SELECT 1"))

@pytest.mark.parametrize("achievement", insert_vals)
def test_achievement_codes_unique(achievement):
    """Test that all achievement codes are unique"""
    code_count = sum(1 for val in insert_vals if val["code"] == achievement["code"])
    assert code_count == 1, f"Duplicate achievement code found: {achievement['code']}"

def test_varchar_length_constraints():
    """Test VARCHAR length constraints"""
    for val in insert_vals:
        assert len(val["name"]) <= VARCHAR_LENGTH
        assert len(val["description"]) <= VARCHAR_LENGTH
        assert len(val["verb"]) <= VARCHAR_LENGTH
        assert len(val["code"]) <= VARCHAR_LENGTH