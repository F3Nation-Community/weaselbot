from sqlalchemy import Table, Column, ForeignKey, MetaData, func, text, select
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, DATE, DATETIME, insert

from f3_data_builder import mysql_connection
from create_view import view

engine = mysql_connection()
metadata = MetaData()

schema = 'f3chicago'
MYSQL_ENGINE = "InnoDB"
MYSQL_CHARSET = "utf8mb3"
MYSQL_COLLATE = "utf8mb3_general_ci"

Table(
    'achievements_list',
    metadata,
    Column('id', INTEGER(), primary_key=True, nullable=False),
    Column('name', VARCHAR(charset='utf8', length=255), nullable=False),
    Column('description', VARCHAR(charset='utf8', length=255), nullable=False),
    Column('verb', VARCHAR(charset='utf8', length=255), nullable=False),
    Column('code', VARCHAR(charset='utf8', length=255), nullable=False),
    mysql_engine=MYSQL_ENGINE,
    mysql_charset=MYSQL_CHARSET,
    mysql_collate=MYSQL_COLLATE,
    schema=schema,
)
Table(
    'achievements_awarded',
    metadata,
    Column('id', INTEGER(), primary_key=True, nullable=False),
    Column('achievement_id', INTEGER(), ForeignKey(f'{schema}.achievements_list.id'), nullable=False),
    Column('pax_id', VARCHAR(charset='utf8', length=255), nullable=False),
    Column('date_awarded', DATE(), nullable=False),
    Column('created', DATETIME(), nullable=False, server_default=func.current_timestamp()),
    Column('updated', DATETIME(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
    mysql_engine=MYSQL_ENGINE,
    mysql_charset=MYSQL_CHARSET,
    mysql_collate=MYSQL_COLLATE,
    schema=schema,
)

with engine.begin() as cnxn:
    metadata.drop_all(cnxn)
    metadata.create_all(cnxn)

u = Table("users", metadata, autoload_with=engine, schema=schema)
aa = metadata.tables[f"{schema}.achievements_awarded"]
al = metadata.tables[f"{schema}.achievements_list"]

stuff_view = view(
        "stuff_view",
        metadata,
        select(
            u.c.user_name.label("pax"),
            u.c.user_id.label("pax_id"),
            al.c.name.label("name"),
            al.c.description.label("description"),
            aa.c.date_awarded.label("date_awarded")
            )
        .select_from(u.join(al, u.c.user_id == aa.c.pax_id)
                     .join(al, aa.c.achievement_id == al.c.id))
    )

stuff_view.create(engine)

insert_vals = [
    {
        'name': 'The Priest',
        'description': 'Post for 25 Qsource lessons',
        'verb': 'posting for 25 Qsource lessons',
        'code': 'the_priest',
    },
    {
        'name': 'The Monk',
        'description': 'Post at 4 QSources in a month',
        'verb': 'posting at 4 Qsources in a month',
        'code': 'the_monk',
    },
    {
        'name': 'Leader of Men',
        'description': 'Q at 4 beatdowns in a month',
        'verb': 'Qing at 4 beatdowns in a month',
        'code': 'leader_of_men',
    },
    {
        'name': 'The Boss',
        'description': 'Q at 6 beatdowns in a month',
        'verb': 'Qing at 6 beatdowns in a month',
        'code': 'the_boss',
    },
    {
        'name': 'Be the Hammer, Not the Nail',
        'description': 'Q at 6 beatdowns in a week',
        'verb': 'Qing at 6 beatdowns in a week',
        'code': 'be_the_hammer_not_the_nail',
    },
    {
        'name': 'Cadre',
        'description': 'Q at 7 different AOs in a month',
        'verb': 'Qing at 7 different AOs in a month',
        'code': 'cadre',
    },
    {
        'name': 'El Presidente',
        'description': 'Q at 20 beatdowns in a year',
        'verb': 'Qing at 20 beatdowns in a year',
        'code': 'el_presidente',
    },
    {
        'name': 'El Quatro',
        'description': 'Post at 25 beatdowns in a year',
        'verb': 'posting at 25 beatdowns in a year',
        'code': 'el_quatro',
    },
    {
        'name': 'Golden Boy',
        'description': 'Post at 50 beatdowns in a year',
        'verb': 'posting at 50 beatdowns in a year',
        'code': 'golden_boy',
    },
    {
        'name': 'Centurion',
        'description': 'Post at 100 beatdowns in a year',
        'verb': 'posting at 100 beatdowns in a year',
        'code': 'centurion',
    },
    {
        'name': 'Karate Kid',
        'description': 'Post at 150 beatdowns in a year',
        'verb': 'posting at 150 beatdowns in a year',
        'code': 'karate_kid',
    },
    {
        'name': 'Crazy Person',
        'description': 'Post at 200 beatdowns in a year',
        'verb': 'posting at 200 beatdowns in a year',
        'code': 'crazy_person',
    },
    {
        'name': '6 pack',
        'description': 'Post at 6 beatdowns in a week',
        'verb': 'posting at 6 beatdowns in a week',
        'code': '6_pack',
    },
    {
        'name': 'Holding Down the Fort',
        'description': 'Post 50 times at an AO',
        'verb': 'posting 50 times at an AO',
        'code': 'holding_down_the_fort',
    },
    # {
    #     'name': 'You aint Cheatin, you ain’t Tryin',
    #     'description': 'Complete a GrowRuck',
    #     'verb': 'completing a GrowRuck',
    #     'code': 'you_aint_cheatin_you_aint_tryin',
    # },
    # {
    #     'name': 'Fall Down, Get up, Together',
    #     'description': 'Complete MABA (3100 burpees)',
    #     'verb': 'completing MABA (>3100 burpees)',
    #     'code': 'fall_down_get_up_together',
    # },
    # {
    #     'name': 'Redwood Original',
    #     'description': 'Post for an inaugural beatdown for an AO launch',
    #     'verb': 'posting at an inaurgural beatdown for an AO launch',
    #     'code': 'redwood_original',
    # },
    # {
    #     'name': 'In This Together',
    #     'description': 'Participate in a shieldlock',
    #     'verb': 'participating in a shieldlock',
    #     'code': 'in_this_together',
    # },
    # {
    #     'name': 'Sleeper Hold',
    #     'description': 'EH and VQ 2 FNGs',
    #     'verb': 'EHing and VQing 2 FNGs',
    #     'code': 'sleeper_hold',
    # },
    # {'name': 'Leave no Man Behind', 'description': 'EH 5 FNGs', 'verb': 'EHing 5 FNGs', 'code': 'leave_no_man_behind'},
]

t = metadata.tables[f"{schema}.achievements_list"]
sql = insert(t).values(insert_vals)

with engine.begin() as cnxn:
    cnxn.execute(sql)

engine.dispose()