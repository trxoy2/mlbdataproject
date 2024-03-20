

def lookup_ozzie(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    ozziealbiesID = playerid_lookup('albies', 'ozzie')

    ozziealbiesStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(ozziealbiesID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ozziealbiesStats', value=ozziealbiesStats)

def lookup_ronald(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    ronaldacunaID = playerid_lookup('acuña', 'ronald')

    ronaldacunaStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(ronaldacunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ronaldacunaStats', value=ronaldacunaStats)

def lookup_olson(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    mattolsonID = playerid_lookup('olson', 'matt')

    mattolsonStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(mattolsonID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='mattolsonStats', value=mattolsonStats)

def lookup_riley(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    austinrileyID = playerid_lookup('riley', 'austin')

    austinrileyStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(austinrileyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='austinrileyStats', value=austinrileyStats)

def lookup_ozuna(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    marcellozunaID = playerid_lookup('ozuna', 'marcell')

    marcellozunaStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(marcellozunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='marcellozunaStats', value=marcellozunaStats)

def lookup_harris(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    michaelharrisID = playerid_lookup('harris', 'michael')

    michaelharrisStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(michaelharrisID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='michaelharrisStats', value=michaelharrisStats)

def lookup_murphy(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    seanmurphyID = playerid_lookup('murphy', 'sean')

    seanmurphyStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(seanmurphyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='seanmurphyStats', value=seanmurphyStats)

def lookup_kelenic(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    jarredkelenicID = playerid_lookup('kelenic', 'jarred')

    jarredkelenicStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(jarredkelenicID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='jarredkelenicStats', value=jarredkelenicStats)

def lookup_arcia(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    orlandoarciaID = playerid_lookup('arcia', 'orlando')

    orlandoarciaStats = statcast_batter('2023-07-01', '2023-07-01', player_id = int(orlandoarciaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='orlandoarciaStats', value=orlandoarciaStats)

def combine_players(ti):
    import pandas as pd

    ozziealbiesStats = ti.xcom_pull(task_ids='collect_players.lookup_ozzie', key='ozziealbiesStats')
    ronaldacunaStats = ti.xcom_pull(task_ids='collect_players.lookup_ronald', key='ronaldacunaStats')
    mattolsonStats = ti.xcom_pull(task_ids='collect_players.lookup_olson', key='mattolsonStats')
    austinrileyStats = ti.xcom_pull(task_ids='collect_players.lookup_riley', key='austinrileyStats')
    marcellozunaStats = ti.xcom_pull(task_ids='collect_players.lookup_ozuna', key='marcellozunaStats')
    michaelharrisStats = ti.xcom_pull(task_ids='collect_players.lookup_harris', key='michaelharrisStats')
    seanmurphyStats = ti.xcom_pull(task_ids='collect_players.lookup_murphy', key='seanmurphyStats')
    jarredkelenicStats = ti.xcom_pull(task_ids='collect_players.lookup_kelenic', key='jarredkelenicStats')
    orlandoarciaStats = ti.xcom_pull(task_ids='collect_players.lookup_kelenic', key='orlandoarciaStats')

    bravesStats = pd.concat([ronaldacunaStats, ozziealbiesStats, mattolsonStats, austinrileyStats, marcellozunaStats, seanmurphyStats, jarredkelenicStats, orlandoarciaStats, michaelharrisStats], ignore_index=True)

    bravesStats['events'].fillna('blank', inplace=True)
    bravesStats = bravesStats.loc[bravesStats['events'] != 'blank']
    print(f"Type of data: {type(bravesStats)}")

    #return bravesStats
    ti.xcom_push(key='bravesStats', value=bravesStats)


def download_pitchdata():
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='BUCKET')

    local_file_path='/opt/airflow/data'
    filename = 'BravesPitchData.csv'

    #download CSV from S3
    s3filename = s3_hook.download_file(
        bucket_name='mlbdata1',
        key=filename,
        local_path =local_file_path,
    )

    return s3filename


def rename_pitchdata(ti):
    import pandas as pd
    import os
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    filename = 'BravesPitchData.csv'
    newfilename = '/opt/airflow/data/BravesPitchData.csv'

    downloadedfilename = ti.xcom_pull(task_ids='download_pitchdata')
    print(downloadedfilename)
    #downloadedfilepath = '/'.join(downloadedfilename[0].split('/')[:-1])
    #print(downloadedfilepath)
    os.rename(downloadedfilename, newfilename)

    BravesPitchDataOld_df = pd.read_csv(newfilename)

    ti.xcom_push(key='BravesPitchDataOld_df', value=BravesPitchDataOld_df)



def transform_bravesStats(ti):
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    BravesPitchDataOld_df = ti.xcom_pull(task_ids='rename_pitchdata', key='BravesPitchDataOld_df')
    BravesPitchDataNew_df = ti.xcom_pull(task_ids='combine_players', key='bravesStats')

    filename = 'BravesPitchData.csv'

    BravesPitchDataNew_df = pd.concat([BravesPitchDataOld_df, BravesPitchDataNew_df], ignore_index=True)
    print(BravesPitchDataNew_df)
    BravesPitchDataNew_df.to_csv(filename, index=False)

    #transform data
    #transformbravesStats_df = pd.read_csv('bravesStats20240224.csv')
    BravesSummarizedData_df = BravesPitchDataNew_df.loc[BravesPitchDataNew_df['events'] == 'home_run']


    print(BravesSummarizedData_df)

    ti.xcom_push(key='BravesSummarizedData_df', value=BravesSummarizedData_df)



def upload_bravesStats(ti):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    BravesSummarizedData_df = ti.xcom_pull(task_ids='transform_bravesStats', key='BravesSummarizedData_df')

    print(BravesSummarizedData_df)

    filename = 'BravesSummarizedData.csv'
    # Convert DataFrame to CSV string
    BravesSummarizedData_df.to_csv(filename, index=False)

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='BUCKET')

    # Upload the CSV data to the specified S3 bucket and object (file) name
    s3_hook.load_file(
        filename =filename,
        key=filename,
        bucket_name='mlbdata1',
        replace=True,
    )

    return f'Successfully saved CSV to S3:mlbdata1/{filename}'
