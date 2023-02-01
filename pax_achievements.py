###!/mnt/nas/ml/f3-analytics/env/bin/python

from re import T, template
import pandas as pd
import numpy as np
from datetime import date
from functools import reduce
import ssl
from slack_sdk import WebClient
from sqlalchemy import create_engine
import re
import os
from dotenv import load_dotenv
from slack_sdk.errors import SlackApiError

# Inputs
year_select = date.today().year

# Gather creds
dummy = load_dotenv()
DATABASE_USER = os.environ.get('DATABASE_USER')
DATABASE_PASSWORD = os.environ.get('DATABASE_PASSWORD')
DATABASE_HOST = os.environ.get('DATABASE_HOST')
engine = create_engine(f'mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:3306')

# Pull paxminer region data
with engine.connect() as conn:
    df_regions = pd.read_sql_query(sql="SELECT * FROM weaselbot.regions WHERE send_achievements = 1;", con=conn)

# Loop through regions
for region_index, region_row in df_regions.iterrows():

    db = region_row['paxminer_schema']
    slack_secret = region_row['slack_token']
    achievement_channel = region_row['achievement_channel']
    
    print(f"running {db}...")

    # Pull region data
    sql_select = f"""-- sql
    SELECT bd.user_id AS pax_id,
        u.user_name AS pax,
        bd.ao_id AS ao_id,
        a.ao,
        bd.date,
        YEAR(bd.date) AS year_num,
        MONTH(bd.date) AS month_num,
        WEEK(bd.date) AS week_num,
        CASE WHEN bd.user_id = bd.q_user_id OR bd.user_id = b.coq_user_id THEN 1 ELSE 0 END AS q_flag,
        b.backblast
    FROM {db}.bd_attendance bd
    INNER JOIN {db}.users u
    ON bd.user_id = u.user_id
    INNER JOIN {db}.aos a
    ON bd.ao_id = a.channel_id
    INNER JOIN {db}.beatdowns b
    ON bd.ao_id = b.ao_id
        AND bd.date = b.bd_date
        AND bd.q_user_id = b.q_user_id
    WHERE YEAR(bd.date) = {year_select}
    ;
    """
    
    awarded_view = f"""-- sql
    SELECT aa.*, al.code
    FROM {db}.achievements_awarded aa
    INNER JOIN {db}.achievements_list al
    ON aa.achievement_id = al.id
    ;
    """

    # Import data from SQL
    with engine.connect() as conn:
        df = pd.read_sql_query(sql=sql_select, con=conn, parse_dates=['date'])
        achievement_list = pd.read_sql_table(table_name="achievements_list", schema=db, con=conn)
        awarded_table = pd.read_sql_query(sql=awarded_view, con=conn, parse_dates=['date_awarded'])
        
        paxminer_log_channel = conn.execute(f"SELECT channel_id FROM {db}.aos WHERE ao = 'paxminer_logs';").fetchone()[0]

    # Create flags for different event types (beatdowns, blackops, qsource, etc)
    # As created below, the split requires the qsource, blackops, rucking channels to be named properly
    # df['qsource_flag'] = False
    # df.loc[df.ao=='qsource', 'qsource_flag'] = True

    # df['blackops_flag'] = False
    # df.loc[df.ao=='blackops', 'blackops_flag'] = True

    # df['ruck_flag'] = False
    # df.loc[df.ao=='rucking', 'ruck_flag'] = True
    
    # Alternatively, here's some code I use for our region that usese both the ao title and tries to guess based on keywords in the backblast title
    df['backblast_title'] = df['backblast'].str.replace('Slackblast: \n','')
    df['backblast_title'] = df['backblast_title'].str.split('\n', expand=True).iloc[:,0]
    
    df['ruck_flag'] = df['backblast_title'].str.contains(r'\b(?:pre-ruck|preruck)\b', flags=re.IGNORECASE, regex=True)
    df.loc[df['ao']=='rucking', 'ruck_flag'] = True

    df['qsource_flag'] = df['backblast_title'].str.contains(r'\b(?:qsource)\b', flags=re.IGNORECASE, regex=True) | \
    df['backblast_title'].str.contains(r'q[1-9]\.[1-9]', flags=re.IGNORECASE, regex=True) | \
    df['backblast_title'].str.contains(r'\b(?:q source)\b', flags=re.IGNORECASE, regex=True)
    df.loc[df['ao']=='qsource', 'qsource_flag'] = True

    df['blackops_flag'] = df['backblast_title'].str.contains(r'\b(?:blackops)\b', flags=re.IGNORECASE, regex=True)
    df.loc[df['ao']=='blackops', 'blackops_flag'] = True
    df.loc[df['ao']=='csaup', 'blackops_flag'] = True
    df.loc[df['ao']=='downrange', 'blackops_flag'] = True

    # Anything that's not a blackops / qsource / ruck is assumed to be a beatdown for counting achievements
    df['bd_flag'] = ~df.blackops_flag & ~df.qsource_flag & ~df.ruck_flag

    # Find manually tagged achievements
    df['achievement'] = df['backblast'].str.extract(r'((?<=achievement:).*(?=\n|$))', flags=re.IGNORECASE)[0].str.strip().str.lower()

    # Change q_flag definition to only include qs for beatdowns
    df.q_flag = df.q_flag & df.bd_flag 

    # instantiate Slack client
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    slack_client = WebClient(slack_secret, ssl=ssl_context)

    # Periodic aggregations for automatic achievement tagging - weekly, monthly, yearly
    # "view" tables are aggregated at the level they are calculated, "agg" tables aggregate them to the annual / pax level for joining together

    #####################################
    #           Weekly views            #
    #####################################
    # Beatdowns only, ao / week level
    pax_week_ao_view = df[df.bd_flag].groupby(
        ['week_num', 'ao_id', 'pax_id']
        )[['bd_flag', 'q_flag']].sum().rename(
        columns={
            'bd_flag':'bd', 'q_flag':'q'
        }
    )
    # Week level
    pax_week_view = pax_week_ao_view.groupby(
        ['week_num', 'pax_id'])[['bd', 'q']].agg(
        ['sum', np.count_nonzero])
    pax_week_view.columns = pax_week_view.columns.map('_'.join).str.strip('_')
    pax_week_view.rename(
        columns={
            'bd_sum':'bd_sum_week', 'q_sum':'q_sum_week',
            'bd_count_nonzero':'bd_ao_count_week', 'q_count_nonzero':'q_ao_count_week'
        },
        inplace=True
    )
    # Aggregate to pax level
    pax_week_agg = pax_week_view.groupby(
        ['pax_id']
        ).max().rename(
            columns={
                'bd_sum_week':'bd_sum_week_max', 'q_sum_week':'q_sum_week_max',
                'bd_ao_count_week':'bd_ao_count_week_max', 'q_ao_count_week':'q_ao_count_week_max'
            }
    )
    # Special counts for travel bonus
    pax_week_view2 = pax_week_view
    pax_week_view2['bd_ao_count_week_extra'] = pax_week_view2['bd_ao_count_week'] - 1
    pax_week_agg2 = pax_week_view.groupby(
        ['pax_id']
        )[['bd_ao_count_week_extra']].sum().rename(
            columns={
                'bd_ao_count_week_extra':'bd_ao_count_week_extra_year'
            }
    )

    # QSources (only once/week counts for points)
    pax_week_other_view = df[df.qsource_flag][['pax_id', 'week_num', 'qsource_flag']].drop_duplicates()
    pax_week_other_agg = pax_week_other_view.groupby(
        ['pax_id']
        )[['qsource_flag']].count().rename(
            columns={
                'qsource_flag':'qsource_week_count'
            }
        )
    # Count total posts per week (including backops)
    pax_week_other_view2  = df.groupby(
        ['week_num', 'pax_id'])[['bd_flag', 'blackops_flag']].sum()
    pax_week_other_view2['bd_blackops_week'] = pax_week_other_view2['bd_flag'] + pax_week_other_view2['blackops_flag']
    pax_week_other_agg2 = pax_week_other_view2.groupby(
        ['pax_id'])[['bd_blackops_week']].max().rename(
            columns={
                'bd_blackops_week': 'bd_blackops_week_max'
            }
        )


    ######################################
    #           Monthly views            #
    ######################################
    # Beatdowns only , month / ao level
    pax_month_ao_view = df[df.bd_flag].groupby(
        ['month_num', 'ao_id', 'pax_id']
        )[['bd_flag', 'q_flag']].sum().rename(
        columns={
            'bd_flag':'bd', 'q_flag':'q'
        }
    )
    # Month level
    pax_month_view = pax_month_ao_view.groupby(
        ['month_num', 'pax_id']
        )[['bd', 'q']].agg(
            ['sum', np.count_nonzero])
    pax_month_view.columns = pax_month_view.columns.map('_'.join).str.strip('_')
    pax_month_view.rename(
        columns={
            'bd_sum':'bd_sum_month', 'q_sum':'q_sum_month',
            'bd_count_nonzero':'bd_ao_count_month', 'q_count_nonzero':'q_ao_count_month'
        },
        inplace=True
    )
    # Monthly (not just beatdowns, includes QSources and Blackops)
    pax_month_view_other = df.groupby(
        ['month_num', 'pax_id']
        )[['qsource_flag', 'blackops_flag']].sum().rename(
        columns={
            'qsource_flag':'qsource_sum_month', 'blackops_flag':'blackops_sum_month'
        }
    )
    # Aggregate to PAX level
    pax_month_agg = pax_month_view.groupby(
        ['pax_id']
        ).max().rename(
            columns={
                'bd_sum_month':'bd_sum_month_max', 'q_sum_month':'q_sum_month_max',
                'bd_ao_count_month':'bd_ao_count_month_max', 'q_ao_count_month':'q_ao_count_month_max'
            }
    )
    pax_month_other_agg = pax_month_view_other.groupby(
        ['pax_id']
        ).max().rename(
            columns={
                'qsource_sum_month':'qsource_sum_month_max',
                'blackops_sum_month':'blackops_sum_month_max'
            }
    )
    # Number of unique AOs Q count
    pax_month_q_view = df[df.q_flag].drop_duplicates(['month_num', 'pax_id', 'ao_id'])
    pax_month_q_view2 = pax_month_q_view.groupby(
        ['month_num', 'pax_id']
        )[['q_flag']].count().rename(
        columns={
            'q_flag':'q_ao_count'
        }
    )
    pax_month_q_agg = pax_month_q_view2.groupby(
        ['pax_id']
        ).max().rename(
        columns={
            'q_ao_count':'q_ao_month_max'
        }
    )

    #####################################
    #           Annual views            #
    #####################################
    # Beatdowns only, ao / annual level
    pax_year_ao_view = df[df.bd_flag].groupby(
        ['ao_id', 'pax_id']
        )[['bd_flag', 'q_flag']].sum().rename(
        columns={
            'bd_flag':'bd', 'q_flag':'q'
        }
    )
    pax_year_view = pax_year_ao_view.groupby(
        ['pax_id']
        )[['bd', 'q']].agg(
        ['sum', np.count_nonzero])
    pax_year_view.columns = pax_year_view.columns.map('_'.join).str.strip('_')
    pax_year_view.rename(
        columns={
            'bd_sum':'bd_sum_year', 'q_sum':'q_sum_year', 
            'bd_count_nonzero':'bd_ao_count_year', 'q_count_nonzero':'q_ao_count_year'
        },
        inplace=True
    )
    # Other than beatdowns
    pax_year_view_other = df.groupby(
        ['pax_id']
        )[['qsource_flag', 'blackops_flag']].sum().rename(
        columns={
            'qsource_flag':'qsource_sum_year', 'blackops_flag':'blackops_sum_year'
        }
    )
    pax_year_ao_view = df[df.bd_flag].groupby(
        ['pax_id', 'ao_id']
        )[['bd_flag']].count().rename(
        columns={
            'bd_flag':'bd_sum_ao'
        }
    )
    pax_year_ao_agg = pax_year_ao_view.groupby(
        ['pax_id']
        )[['bd_sum_ao']].max().rename(
        columns={
            'bd_sum_ao':'bd_sum_ao_max'
        }
    )


    # Merge everything to PAX / annual view
    pax_name_df = df.groupby('pax_id', as_index=False)['pax'].first()
    merge_list = [
        pax_name_df,
        pax_year_view_other,
        pax_year_view,
        pax_year_ao_agg,
        pax_month_other_agg,
        pax_month_q_agg,
        pax_month_agg,
        pax_week_agg,
        pax_week_other_agg,
        pax_week_agg2,
        pax_week_other_agg2
    ]
    pax_view = reduce(lambda left,right: pd.merge(left, right, on=['pax_id'], how='outer'), merge_list).fillna(0)

    # Calculate automatic achievements
    pax_view['the_priest'] = pax_view['qsource_sum_year'] >= 25
    pax_view['the_monk'] = pax_view['qsource_sum_month_max'] >= 4
    pax_view['leader_of_men'] = pax_view['q_sum_month_max'] >= 4
    pax_view['the_boss'] = pax_view['q_sum_month_max'] >= 6
    pax_view['be_the_hammer_not_the_nail'] = pax_view['q_sum_week_max'] >= 6
    pax_view['cadre'] = pax_view['q_ao_month_max'] >= 7
    pax_view['road_warrior'] = pax_view['bd_ao_count_month_max'] >= 10
    pax_view['el_presidente'] = pax_view['q_sum_year'] >= 20
    pax_view['6_pack'] = pax_view['bd_blackops_week_max'] >= 6
    pax_view['el_quatro'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 25
    pax_view['golden_boy'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 50
    pax_view['centurion'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 100
    pax_view['karate_kid'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 150
    pax_view['crazy_person'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 200
    pax_view['holding_down_the_fort'] = pax_view['bd_sum_ao_max'] >= 50

    # Flag manual acheivements from tagged backblasts
    man_achievement_df = df.loc[~(df.achievement.isna()), ['pax_id', 'achievement']].drop_duplicates(['pax_id', 'achievement'])
    man_achievement_df['achieved'] = True
    man_achievement_df = man_achievement_df.pivot(index=['pax_id'], columns=['achievement'], values=['achieved'])

    # Merge to PAX view
    man_achievement_df = man_achievement_df.droplevel(0, axis=1).reset_index()
    pax_view = pd.merge(pax_view, man_achievement_df, on=['pax_id'], how='left')

    # Reshape awarded table and merge
    awarded_table = awarded_table.pivot(index='pax_id', columns='code', values='date_awarded').reset_index()
    awarded_table.set_index('pax_id', inplace=True)
    # awarded_table.columns = [x + '_awarded' for x in awarded_table.columns]
    pax_view = pd.merge(pax_view, awarded_table, how='left', on='pax_id', suffixes=("", "_awarded"))

    # Loop through achievement list, looking for achievements earned but not yet awarded
    award_count = 0
    awards_add = pd.DataFrame(columns=['pax_id', 'achievement_id', 'date_awarded'])
    
    for index, row in achievement_list.iterrows():
        award = row['code']
        
        # check to see if award has been earned anywhere and / or has been awarded
        if award + '_awarded' in pax_view.columns:
            new_awards = pax_view[(pax_view[award] == True) & (pax_view[award + '_awarded'].isna())]
        elif award in pax_view.columns:
            new_awards = pax_view[pax_view[award] == True]
        else:
            new_awards = pd.DataFrame()

        if len(new_awards) > 0:
            for pax_index, pax_row in new_awards.iterrows():
                # mark off in the awarded table as awarded for that PAX
                awards_add.loc[len(awards_add.index)] = [pax_row['pax_id'], row['id'], date.today()]
                achievements_to_date = len(awarded_table[awarded_table.index==pax_row['pax_id']]) + len(awards_add[awards_add['pax_id']==pax_row['pax_id']])
                
                # send to slack channel
                sMessage = f"Congrats to our man <@{pax_row['pax_id']}>! He just unlocked the achievement *{row['name']}* for {row['verb']}. This is achievement #{achievements_to_date} for <@{pax_row['pax_id']}> this year. Keep up the good work!"
                print(sMessage)
                try:
                    response = slack_client.chat_postMessage(channel=achievement_channel, text=sMessage, link_names=True)
                except SlackApiError as e:
                    slack_client.conversations_join(channel=achievement_channel)
                    response = slack_client.chat_postMessage(channel=achievement_channel, text=sMessage, link_names=True)
                response2 = slack_client.reactions_add(channel=achievement_channel, name='fire', timestamp=response['ts'])

    # Append new awards to award table
    with engine.connect() as conn:
        awards_add.to_sql(name='achievements_awarded', con=conn, if_exists='append', index=False, schema=db)

    # Send confirmation message to paxminer log channel
    try:
        response = slack_client.chat_postMessage(channel=paxminer_log_channel, text=f'Patch program run for the day, {len(awards_add)} awards tagged')
    except SlackApiError as e:
        slack_client.conversations_join(channel=paxminer_log_channel)
        response = slack_client.chat_postMessage(channel=paxminer_log_channel, text=f'Patch program run for the day, {len(awards_add)} awards tagged')
    
    print("All done!")