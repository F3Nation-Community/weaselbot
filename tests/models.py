from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy.sql.schema import ScalarElementColumnDefault
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, TINYINT, DATE, JSON, DOUBLE, TEXT, LONGTEXT

Table(
    'regions',
    MetaData(),
    Column('region', VARCHAR(length=45), primary_key=True, nullable=False),
    Column('slack_token', VARCHAR(length=90), nullable=False),
    Column('schema_name', VARCHAR(length=45)),
    Column('active', TINYINT(), default=ScalarElementColumnDefault(1)),
    Column('firstf_channel', VARCHAR(length=45)),
    Column('contact', VARCHAR(length=45)),
    Column('send_pax_charts', TINYINT(), default=ScalarElementColumnDefault(0)),
    Column('send_ao_leaderboard', TINYINT(), default=ScalarElementColumnDefault(0)),
    Column('send_q_charts', TINYINT(), default=ScalarElementColumnDefault(0)),
    Column('send_region_leaderboard', TINYINT(), default=ScalarElementColumnDefault(0)),
    Column('send_region_uniquepax_chart', TINYINT(), default=ScalarElementColumnDefault(0)),
    Column('send_region_stats', VARCHAR(length=45), default=ScalarElementColumnDefault(0)),
    Column('send_mid_month_charts', VARCHAR(length=45), default=ScalarElementColumnDefault(0)),
    Column('comments', TEXT()),
    schema='paxminer',
)
Table(
    'combined_regions',
    MetaData(),
    Column('region_id', INTEGER(), primary_key=True, nullable=False),
    Column('region_name', VARCHAR(length=45), nullable=False),
    Column('schema_name', VARCHAR(length=45), nullable=False),
    Column('slack_team_id', VARCHAR(length=20)),
    Column('max_timestamp', DOUBLE(asdecimal=True)),
    Column('max_ts_edited', DOUBLE(asdecimal=True)),
    schema='weaselbot',
)
Table(
    'regions',
    MetaData(),
    Column('id', INTEGER(), primary_key=True, nullable=False),
    Column('paxminer_schema', VARCHAR(length=100), nullable=False),
    Column('slack_token', VARCHAR(length=100), nullable=False),
    Column('send_achievements', TINYINT(), nullable=False, for_update=False),
    Column('send_aoq_reports', TINYINT(), nullable=False, for_update=False),
    Column('achievement_channel', VARCHAR(length=100), nullable=False),
    Column('default_siteq', VARCHAR(length=45)),
    schema='weaselbot',
)
Table(
    'test_json',
    MetaData(),
    Column('pax', VARCHAR(length=100)),
    Column('date', DATE()),
    Column('extras', JSON()),
    schema='weaselbot',
)
Table(
    'combined_aos',
    MetaData(),
    Column('ao_id', INTEGER(), primary_key=True, nullable=False),
    Column('slack_channel_id', VARCHAR(length=45), nullable=False),
    Column('ao_name', VARCHAR(length=45), nullable=False),
    Column('region_id', INTEGER(), ForeignKey('weaselbot.combined_regions.region_id'), nullable=False),
    schema='weaselbot',
)
Table(
    'combined_users',
    MetaData(),
    Column('user_id', INTEGER(), primary_key=True, nullable=False),
    Column('user_name', VARCHAR(length=45), nullable=False),
    Column('email', VARCHAR(length=45), nullable=False),
    Column('home_region_id', INTEGER(), ForeignKey('weaselbot.combined_regions.region_id'), nullable=False),
    schema='weaselbot',
)
Table(
    'combined_beatdowns',
    MetaData(),
    Column('beatdown_id', INTEGER(), primary_key=True, nullable=False),
    Column('ao_id', INTEGER(), ForeignKey('weaselbot.combined_aos.ao_id'), nullable=False),
    Column('bd_date', DATE(), nullable=False),
    Column('q_user_id', INTEGER(), ForeignKey('weaselbot.combined_users.user_id')),
    Column('coq_user_id', INTEGER(), ForeignKey('weaselbot.combined_users.user_id')),
    Column('pax_count', INTEGER()),
    Column('fng_count', INTEGER()),
    Column('timestamp', DOUBLE(asdecimal=True)),
    Column('ts_edited', DOUBLE(asdecimal=True)),
    Column('backblast', LONGTEXT()),
    Column('json', JSON()),
    schema='weaselbot',
)
Table(
    'combined_users_dup',
    MetaData(),
    Column('user_id_dup', INTEGER(), primary_key=True, nullable=False),
    Column('slack_user_id', VARCHAR(length=45)),
    Column('user_name', VARCHAR(length=45), nullable=False),
    Column('email', VARCHAR(length=45), nullable=False),
    Column('region_id', INTEGER(), ForeignKey('weaselbot.combined_regions.region_id'), nullable=False),
    Column('user_id', INTEGER(), ForeignKey('weaselbot.combined_users.user_id'), nullable=False),
    schema='weaselbot',
)
Table(
    'combined_attendance',
    MetaData(),
    Column('attendance_id', INTEGER(), primary_key=True, nullable=False),
    Column('beatdown_id', INTEGER(), ForeignKey('weaselbot.combined_beatdowns.beatdown_id'), nullable=False),
    Column('user_id', INTEGER(), ForeignKey('weaselbot.combined_users.user_id'), nullable=False),
    Column('json', JSON()),
    schema='weaselbot',
)
