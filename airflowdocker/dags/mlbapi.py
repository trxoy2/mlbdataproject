

def lookup_ozzie(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter

    ozziealbiesID = playerid_lookup('albies', 'ozzie')

    ozziealbiesStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(ozziealbiesID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ozziealbiesStats', value=ozziealbiesStats)

def lookup_ronald(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter

    ronaldacunaID = playerid_lookup('acuña', 'ronald')

    ronaldacunaStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(ronaldacunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ronaldacunaStats', value=ronaldacunaStats)

def lookup_olson(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter

    mattolsonID = playerid_lookup('olson', 'matt')

    mattolsonStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(mattolsonID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='mattolsonStats', value=mattolsonStats)

def lookup_riley(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter

    austinrileyID = playerid_lookup('riley', 'austin')

    austinrileyStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(austinrileyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='austinrileyStats', value=austinrileyStats)

def combine_players(ti):
    import pandas as pd

    ozziealbiesStats = ti.xcom_pull(task_ids='collect_players.lookup_ozzie', key='ozziealbiesStats')
    ronaldacunaStats = ti.xcom_pull(task_ids='collect_players.lookup_ronald', key='ronaldacunaStats')
    mattolsonStats = ti.xcom_pull(task_ids='collect_players.lookup_olson', key='mattolsonStats')
    austinrileyStats = ti.xcom_pull(task_ids='collect_players.lookup_riley', key='austinrileyStats')

    bravesStats = pd.concat([ozziealbiesStats, ronaldacunaStats, mattolsonStats, austinrileyStats], ignore_index=True)
    
    print(bravesStats)

    bravesStats['events'].fillna('blank', inplace=True)
    bravesStats = bravesStats.loc[bravesStats['events'] != 'blank']
    print(f"Type of data: {type(bravesStats)}")

    ti.xcom_push(key='bravesStats', value=bravesStats)



def transform_bravesStats(ti):
    import pandas as pd


    transformbravesStats = ti.xcom_pull(task_ids='combine_players', key='bravesStats')

    print(transformbravesStats)

    ti.xcom_push(key='transformbravesStats', value=transformbravesStats)



def upload_bravesStats(ti):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime

    bravesStats_df = ti.xcom_pull(task_ids='transform_bravesStats', key='transformbravesStats')

    timestamp = datetime.now().strftime('%Y%m%d')

    print(bravesStats_df)

    filename = 'bravesStats'+timestamp+'.csv'
    # Convert DataFrame to CSV string
    bravesStats_df.to_csv(filename, index=False)

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='s3bucket')

    # Upload the CSV data to the specified S3 bucket and object (file) name
    s3_hook.load_file(
        filename =filename,
        key=filename,
        bucket_name='mlbdata1',
        replace=True,
    )

    return f'Successfully saved CSV to S3:mlbdata1/{filename}'
