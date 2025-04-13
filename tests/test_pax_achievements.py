import pytest
import polars as pl
from datetime import date, datetime
from unittest.mock import MagicMock, patch
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, select
from sqlalchemy.sql import text

from weaselbot.pax_achievements import (
    home_region_sub_query,
    build_home_regions,
    the_priest,
    the_monk,
    leader_of_men,
    six_pack,
    hdtf,
    load_to_database
)

@pytest.fixture
def mock_engine():
    """Create mock SQLAlchemy engine"""
    engine = MagicMock()
    connection = MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    return engine

@pytest.fixture
def mock_metadata():
    """Create MetaData instance for testing"""
    return MetaData()

@pytest.fixture
def sample_data():
    """Create sample DataFrame for testing"""
    return pl.DataFrame({
        'email': ['user1@f3.com', 'user2@f3.com'],
        'user_name': ['User1', 'User2'],
        'ao_id': ['AO1', 'AO2'],
        'ao': ['WorkoutAO', 'QSource'],
        'date': [date(2025, 1, 1), date(2025, 1, 2)],
        'q_flag': [1, 0],
        'backblast': ['Regular workout', 'Q Source 1.1'],
        'region': ['f3region1', 'f3region2']
    })

@pytest.fixture
def mock_tables(mock_engine, mock_metadata):
    """Create mock database tables"""
    users = Table(
        'users', mock_metadata,
        Column('email', String),
        Column('user_id', String),
        schema='test_schema'
    )
    bd_attendance = Table(
        'bd_attendance', mock_metadata,
        Column('user_id', String),
        Column('ao_id', String),
        Column('q_user_id', String),
        Column('date', DateTime),
        schema='test_schema'
    )
    beatdowns = Table(
        'beatdowns', mock_metadata,
        Column('ao_id', String),
        Column('bd_date', DateTime),
        Column('q_user_id', String),
        schema='test_schema'
    )
    aos = Table(
        'aos', mock_metadata,
        Column('channel_id', String),
        Column('ao', String),
        schema='test_schema'
    )
    return users, bd_attendance, beatdowns, aos

def test_home_region_sub_query(mock_tables):
    """Test home region subquery generation"""
    u, a, b, ao = mock_tables
    date_range = 30
    
    query = home_region_sub_query(u, a, b, ao, date_range)
    
    assert "SELECT" in str(query)
    assert "count" in str(query)
    assert "WHERE" in str(query)
    assert "GROUP BY" in str(query)
    assert str(date_range) in str(query.compile(compile_kwargs={"literal_binds": True}))

def test_the_priest_achievement(sample_data):
    """Test The Priest achievement calculation"""
    bb_filter = pl.col('backblast').str.contains('Q Source')
    ao_filter = pl.col('ao').str.contains('QSource')
    
    result = the_priest(sample_data, bb_filter, ao_filter)
    
    assert isinstance(result, pl.DataFrame)
    assert 'email' in result.columns
    assert 'region' in result.columns
    assert 'date_awarded' in result.columns

def test_the_monk_achievement(sample_data):
    """Test The Monk achievement calculation"""
    bb_filter = pl.col('backblast').str.contains('Q Source')
    ao_filter = pl.col('ao').str.contains('QSource')
    
    result = the_monk(sample_data, bb_filter, ao_filter)
    
    assert isinstance(result, pl.DataFrame)
    assert 'month' in result.columns
    assert 'email' in result.columns
    assert 'region' in result.columns

def test_leader_of_men_achievement(sample_data):
    """Test Leader of Men achievement calculation"""
    bb_filter = ~pl.col('backblast').str.contains('Q Source')
    ao_filter = ~pl.col('ao').str.contains('QSource')
    
    result = leader_of_men(sample_data, bb_filter, ao_filter)
    
    assert isinstance(result, pl.DataFrame)
    assert 'month' in result.columns
    assert 'email' in result.columns
    assert 'region' in result.columns

def test_six_pack_achievement(sample_data):
    """Test Six Pack achievement calculation"""
    bb_filter = ~pl.col('backblast').str.contains('Q Source')
    ao_filter = ~pl.col('ao').str.contains('QSource|ruck')
    
    result = six_pack(sample_data, bb_filter, ao_filter)
    
    assert isinstance(result, pl.DataFrame)
    assert 'week' in result.columns
    assert 'email' in result.columns
    assert 'region' in result.columns

def test_hdtf_achievement(sample_data):
    """Test HDTF achievement calculation"""
    bb_filter = ~pl.col('backblast').str.contains('Q Source')
    ao_filter = ~pl.col('ao').str.contains('QSource|ruck')
    
    result = hdtf(sample_data, bb_filter, ao_filter)
    
    assert isinstance(result, pl.DataFrame)
    assert 'year' in result.columns
    assert 'email' in result.columns
    assert 'region' in result.columns

@patch('weaselbot.achievement_tables.mysql_connection')
def test_load_to_database(mock_conn, mock_engine, mock_metadata):
    """Test database loading functionality"""
    schema = 'test_schema'
    data_to_load = pl.DataFrame({
        'achievement_id': [1],
        'pax_id': ['USER1'],
        'date_awarded': [datetime.now()]
    })
    
    # Mock table
    achievement_table = Table(
        'achievements_awarded', mock_metadata,
        Column('achievement_id', Integer),
        Column('pax_id', String),
        Column('date_awarded', DateTime),
        schema=schema
    )
    
    # Test load_to_database function
    load_to_database(schema, mock_engine, mock_metadata, data_to_load)
    
    # Verify execution
    mock_engine.begin.assert_called_once()
    mock_engine.begin().__enter__().execute.assert_called_once()


@pytest.mark.parametrize("date_range", [30, 60, 90, 120])
def test_home_region_date_ranges(mock_tables, date_range):
    """Test home region queries with different date ranges"""
    u, a, b, ao = mock_tables
    query = home_region_sub_query(u, a, b, ao, date_range)
    
    # Compile query once with literals for inspection
    compiled_str = str(query.compile(compile_kwargs={"literal_binds": True}))
    
    # Verify query structure and date range usage
    assert "SELECT" in compiled_str
    assert "FROM" in compiled_str
    assert "WHERE" in compiled_str
    assert "GROUP BY" in compiled_str
    
    # Verify date range appears in DATEDIFF context
    assert f"datediff(curdate(), test_schema.beatdowns.bd_date) < {date_range}" in compiled_str.lower()