"""
This module defines the schema and operations for the achievements tables in a MySQL database using SQLAlchemy.

Functions:
    create_table(name, columns, metadata, schema):
        Creates a SQLAlchemy Table object with the specified name, columns, metadata, and schema.

Tables:
    achievements_list:
        Columns:
            - id: INTEGER, primary key, not nullable
            - name: VARCHAR, not nullable
            - description: VARCHAR, not nullable
            - verb: VARCHAR, not nullable
            - code: VARCHAR, not nullable

    achievements_awarded:
        Columns:
            - id: INTEGER, primary key, not nullable
            - achievement_id: INTEGER, foreign key referencing achievements_list.id, not nullable
            - pax_id: VARCHAR, not nullable
            - date_awarded: DATE, not nullable
            - created: DATETIME, not nullable, default current timestamp
            - updated: DATETIME, not nullable, default current timestamp on update

Variables:
    insert_vals: List of dictionaries containing initial values to be inserted into the achievements_list table.

Operations:
    - Drop all existing tables and create new ones based on the defined schema.
    - Insert initial values into the achievements_list table.
    - Attempt to alter the 'aos' table to add a new column 'site_q_user_id'.
    - Create or replace a view 'achievements_view' that joins users, achievements_awarded, and achievements_list tables.

Usage:
    This module is intended to be executed as a script to set up the achievements tables and initial data in the database.
"""

from sqlalchemy import Column, ForeignKey, MetaData, Table, func, select, text
from sqlalchemy.dialects.mysql import DATE, DATETIME, INTEGER, VARCHAR, insert
from sqlalchemy.exc import ProgrammingError

from .utils import mysql_connection

engine = mysql_connection()
metadata = MetaData()

schema = "f3paulding"
MYSQL_ENGINE = "InnoDB"
MYSQL_CHARSET = "utf8mb3"
MYSQL_COLLATE = "utf8mb3_general_ci"
VARCHAR_CHARSET = "utf8"
VARCHAR_LENGTH = 255


def create_table(name, columns, metadata, schema):
    return Table(
        name,
        metadata,
        *columns,
        mysql_engine=MYSQL_ENGINE,
        mysql_charset=MYSQL_CHARSET,
        mysql_collate=MYSQL_COLLATE,
        schema=schema,
    )


achievements_list_columns = [
    Column("id", INTEGER(), primary_key=True, nullable=False),
    Column("name", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False),
    Column("description", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False),
    Column("verb", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False),
    Column("code", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False),
]

achievements_awarded_columns = [
    Column("id", INTEGER(), primary_key=True, nullable=False),
    Column("achievement_id", INTEGER(), ForeignKey(f"{schema}.achievements_list.id"), nullable=False),
    Column("pax_id", VARCHAR(charset=VARCHAR_CHARSET, length=VARCHAR_LENGTH), nullable=False),
    Column("date_awarded", DATE(), nullable=False),
    Column("created", DATETIME(), nullable=False, server_default=func.current_timestamp()),
    Column("updated", DATETIME(), nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")),
]

achievements_list = create_table("achievements_list", achievements_list_columns, metadata, schema)
achievements_awarded = create_table("achievements_awarded", achievements_awarded_columns, metadata, schema)

insert_vals = [
    {
        "name": "The Priest",
        "description": "Post for 25 Qsource lessons",
        "verb": "posting for 25 Qsource lessons",
        "code": "the_priest",
    },
    {
        "name": "The Monk",
        "description": "Post at 4 QSources in a month",
        "verb": "posting at 4 Qsources in a month",
        "code": "the_monk",
    },
    {
        "name": "Leader of Men",
        "description": "Q at 4 beatdowns in a month",
        "verb": "Qing at 4 beatdowns in a month",
        "code": "leader_of_men",
    },
    {
        "name": "The Boss",
        "description": "Q at 6 beatdowns in a month",
        "verb": "Qing at 6 beatdowns in a month",
        "code": "the_boss",
    },
    {
        "name": "Be the Hammer, Not the Nail",
        "description": "Q at 6 beatdowns in a week",
        "verb": "Qing at 6 beatdowns in a week",
        "code": "be_the_hammer_not_the_nail",
    },
    {
        "name": "Cadre",
        "description": "Q at 7 different AOs in a month",
        "verb": "Qing at 7 different AOs in a month",
        "code": "cadre",
    },
    {
        "name": "El Presidente",
        "description": "Q at 20 beatdowns in a year",
        "verb": "Qing at 20 beatdowns in a year",
        "code": "el_presidente",
    },
    {
        "name": "El Quatro",
        "description": "Post at 25 beatdowns in a year",
        "verb": "posting at 25 beatdowns in a year",
        "code": "el_quatro",
    },
    {
        "name": "Golden Boy",
        "description": "Post at 50 beatdowns in a year",
        "verb": "posting at 50 beatdowns in a year",
        "code": "golden_boy",
    },
    {
        "name": "Centurion",
        "description": "Post at 100 beatdowns in a year",
        "verb": "posting at 100 beatdowns in a year",
        "code": "centurion",
    },
    {
        "name": "Karate Kid",
        "description": "Post at 150 beatdowns in a year",
        "verb": "posting at 150 beatdowns in a year",
        "code": "karate_kid",
    },
    {
        "name": "Crazy Person",
        "description": "Post at 200 beatdowns in a year",
        "verb": "posting at 200 beatdowns in a year",
        "code": "crazy_person",
    },
    {
        "name": "6 pack",
        "description": "Post at 6 beatdowns in a week",
        "verb": "posting at 6 beatdowns in a week",
        "code": "6_pack",
    },
    {
        "name": "Holding Down the Fort",
        "description": "Post 50 times at an AO",
        "verb": "posting 50 times at an AO",
        "code": "holding_down_the_fort",
    },
]

t = metadata.tables[f"{schema}.achievements_list"]
sql = insert(t).values(insert_vals)

with engine.begin() as cnxn:
    metadata.drop_all(cnxn)
    metadata.create_all(cnxn)
    cnxn.execute(sql)
    try:
        cnxn.execute(text(f"ALTER TABLE {schema}.aos ADD site_q_user_id VARCHAR(45) NULL;"))
    except ProgrammingError as e:
        print(e)


u = Table("users", metadata, autoload_with=engine, schema=schema)
aa = metadata.tables[f"{schema}.achievements_awarded"]
al = metadata.tables[f"{schema}.achievements_list"]

sql = select(
    u.c.user_name.label("pax"),
    u.c.user_id.label("pax_id"),
    al.c.name,
    al.c.description,
    aa.c.date_awarded,
).select_from(u.join(aa, u.c.user_id == aa.c.pax_id).join(al, aa.c.achievement_id == al.c.id))

view = f"CREATE OR REPLACE ALGORITHM = UNDEFINED VIEW {schema}.achievements_view AS {sql.compile(engine).__str__()};"

with engine.begin() as cnxn:
    cnxn.execute(text(view))

engine.dispose()
