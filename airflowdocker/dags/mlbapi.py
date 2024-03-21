

def lookup_ozzie(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    ozziealbiesID = playerid_lookup('albies', 'ozzie')

    ozziealbiesStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(ozziealbiesID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ozziealbiesStats', value=ozziealbiesStats)

def lookup_ronald(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    ronaldacunaID = playerid_lookup('acuña', 'ronald')

    ronaldacunaStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(ronaldacunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ronaldacunaStats', value=ronaldacunaStats)

def lookup_olson(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    mattolsonID = playerid_lookup('olson', 'matt')

    mattolsonStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(mattolsonID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='mattolsonStats', value=mattolsonStats)

def lookup_riley(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    austinrileyID = playerid_lookup('riley', 'austin')

    austinrileyStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(austinrileyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='austinrileyStats', value=austinrileyStats)

def lookup_ozuna(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    marcellozunaID = playerid_lookup('ozuna', 'marcell')

    marcellozunaStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(marcellozunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='marcellozunaStats', value=marcellozunaStats)

def lookup_harris(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    michaelharrisID = playerid_lookup('harris', 'michael')

    michaelharrisStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(michaelharrisID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='michaelharrisStats', value=michaelharrisStats)

def lookup_murphy(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    seanmurphyID = playerid_lookup('murphy', 'sean')

    seanmurphyStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(seanmurphyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='seanmurphyStats', value=seanmurphyStats)

def lookup_kelenic(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    jarredkelenicID = playerid_lookup('kelenic', 'jarred')

    jarredkelenicStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(jarredkelenicID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='jarredkelenicStats', value=jarredkelenicStats)

def lookup_arcia(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    import datetime

    #timestamp = datetime.now().strftime('%Y-%m-%d')

    orlandoarciaID = playerid_lookup('arcia', 'orlando')

    orlandoarciaStats = statcast_batter('2023-07-04', '2023-07-04', player_id = int(orlandoarciaID['key_mlbam'].values[0]))

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

    BravesPitchDataToday_df = pd.concat([ronaldacunaStats, ozziealbiesStats, mattolsonStats, austinrileyStats, marcellozunaStats, seanmurphyStats, jarredkelenicStats, orlandoarciaStats, michaelharrisStats], ignore_index=True)

    BravesPitchDataToday_df['events'].fillna('blank', inplace=True)
    BravesPitchDataToday_df = BravesPitchDataToday_df.loc[BravesPitchDataToday_df['events'] != 'blank']
    print(f"Type of data: {type(BravesPitchDataToday_df)}")

    #return bravesStats
    ti.xcom_push(key='BravesPitchDataToday_df', value=BravesPitchDataToday_df)


def download_s3data(ti):
    import pandas as pd
    import os
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='BUCKET')

    local_file_path='/opt/airflow/data'
    filename1 = 'BravesPitchData.csv'
    filename2 = 'BravesSummarizedData.csv'

    #download CSV from S3
    PitchDatafilename = s3_hook.download_file(
        bucket_name='mlbdata1',
        key=filename1,
        local_path =local_file_path,
    )

    SummarizedDatafilename = s3_hook.download_file(
        bucket_name='mlbdata1',
        key=filename2,
        local_path =local_file_path,
    )

    #rename file
    newPitchDatafilename = '/opt/airflow/data/BravesPitchData.csv'
    newSummarizedDatafilename = '/opt/airflow/data/BravesSummarizedData.csv'
    os.rename(PitchDatafilename, newPitchDatafilename)
    os.rename(SummarizedDatafilename, newSummarizedDatafilename)

    BravesPitchDataOld_df = pd.read_csv(newPitchDatafilename)
    BravesSummarizedDataOld_df = pd.read_csv(newSummarizedDatafilename)

    ti.xcom_push(key='BravesPitchDataOld_df', value=BravesPitchDataOld_df)
    ti.xcom_push(key='BravesPitchDataOld_df', value=BravesSummarizedDataOld_df)




def transform_bravesStats(ti):
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    BravesPitchDataOld_df = ti.xcom_pull(task_ids='download_s3data', key='BravesPitchDataOld_df')
    BravesSummarizedDataOld_df = ti.xcom_pull(task_ids='download_s3data', key='BravesSummarizedDataOld_df')
    BravesPitchDataToday_df = ti.xcom_pull(task_ids='combine_players', key='BravesPitchDataToday_df')
    

    BravesPitchDataNew_df = pd.concat([BravesPitchDataOld_df, BravesPitchDataToday_df], ignore_index=True)

    #transform data
    #BravesPitchDataToday_df = pd.read_csv('../data/BravesPitchData.csv')
    BravesSummarizedDataToday_df = BravesPitchDataToday_df.loc[BravesPitchDataToday_df['events'] == 'home_run']
    # Resetting the index
    BravesSummarizedDataToday_df.reset_index(drop=True, inplace=True)

    BravesSummarizedDataNew_df = pd.concat([BravesSummarizedDataOld_df, BravesSummarizedDataToday_df], ignore_index=True)


    print(BravesPitchDataNew_df)

    ti.xcom_push(key='BravesSummarizedDataNew_df', value=BravesSummarizedDataNew_df)
    ti.xcom_push(key='BravesPitchDataNew_df', value=BravesPitchDataNew_df)



def upload_bravesStats(ti):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    BravesSummarizedDataNew_df = ti.xcom_pull(task_ids='transform_bravesStats', key='BravesSummarizedDataNew_df')
    BravesPitchDataNew_df = ti.xcom_pull(task_ids='transform_bravesStats', key='BravesPitchDataNew_df')

    filename1 = 'BravesSummarizedData.csv'
    filename2 = 'BravesPitchData.csv'
    # Convert DataFrame to CSV string
    BravesSummarizedDataNew_df.to_csv(filename1, index=False)
    BravesPitchDataNew_df.to_csv(filename2, index=False)

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='BUCKET')

    # Upload the CSV data to the specified S3 bucket and object (file) name
    s3_hook.load_file(
        filename =filename1,
        key=filename1,
        bucket_name='mlbdata1',
        replace=True,
    )

    s3_hook.load_file(
        filename =filename2,
        key=filename2,
        bucket_name='mlbdata1',
        replace=True,
    )

    return f'Successfully saved CSV to S3:mlbdata1'
