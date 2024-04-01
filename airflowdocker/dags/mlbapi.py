

def lookup_ozzie(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    ozziealbiesID = playerid_lookup('albies', 'ozzie')

    ozziealbiesStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(ozziealbiesID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ozziealbiesStats', value=ozziealbiesStats)

def lookup_ronald(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    ronaldacunaID = playerid_lookup('acuña', 'ronald')

    ronaldacunaStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(ronaldacunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='ronaldacunaStats', value=ronaldacunaStats)

def lookup_olson(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    mattolsonID = playerid_lookup('olson', 'matt')

    mattolsonStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(mattolsonID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='mattolsonStats', value=mattolsonStats)

def lookup_riley(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    austinrileyID = playerid_lookup('riley', 'austin')

    austinrileyStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(austinrileyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='austinrileyStats', value=austinrileyStats)

def lookup_ozuna(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    marcellozunaID = playerid_lookup('ozuna', 'marcell')

    marcellozunaStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(marcellozunaID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='marcellozunaStats', value=marcellozunaStats)

def lookup_harris(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    michaelharrisID = playerid_lookup('harris', 'michael')

    michaelharrisStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(michaelharrisID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='michaelharrisStats', value=michaelharrisStats)

def lookup_murphy(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    seanmurphyID = playerid_lookup('murphy', 'sean')

    seanmurphyStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(seanmurphyID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='seanmurphyStats', value=seanmurphyStats)

def lookup_kelenic(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    jarredkelenicID = playerid_lookup('kelenic', 'jarred')

    jarredkelenicStats = statcast_batter('2024-03-29', '2024-03-29', player_id = int(jarredkelenicID['key_mlbam'].values[0]))

    #return bravesStats
    ti.xcom_push(key='jarredkelenicStats', value=jarredkelenicStats)

def lookup_arcia(ti):
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter
    from datetime import date

    today = date.today().strftime('%Y-%m-%d')

    orlandoarciaID = playerid_lookup('arcia', 'orlando')

    orlandoarciaStats = statcast_batter('2023-07-25', '2023-07-29', player_id = int(orlandoarciaID['key_mlbam'].values[0]))

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
    ti.xcom_push(key='BravesSummarizedDataOld_df', value=BravesSummarizedDataOld_df)




def transform_bravesStats(ti):
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import date

    #today = date.today().strftime('%Y-%m-%d')

    BravesPitchDataOld_df = ti.xcom_pull(task_ids='download_s3data', key='BravesPitchDataOld_df')
    BravesSummarizedDataOld_df = ti.xcom_pull(task_ids='download_s3data', key='BravesSummarizedDataOld_df')
    BravesPitchDataToday_df = ti.xcom_pull(task_ids='combine_players', key='BravesPitchDataToday_df')

    #transform data local
    #BravesPitchDataToday_df = pd.read_csv('../data/BravesPitchData.csv')

    #Create master table for pitch data
    BravesPitchDataNew_df = pd.concat([BravesPitchDataOld_df, BravesPitchDataToday_df], ignore_index=True)

    #Summarize Data
    #Only show pitches where an event happened
    BravesPitchDataToday_df.fillna({'events': 'blank'}, inplace=True)
    BravesPitchDataToday_df = BravesPitchDataToday_df.loc[BravesPitchDataToday_df['events'] != 'blank']

    atbats_df = BravesPitchDataToday_df.groupby('player_name').size().reset_index(name='at_bats')
    #hits
    if (BravesPitchDataToday_df['events'].isin(['single', 'double', 'triple', 'home_run'])).any():
        hits_df = BravesPitchDataToday_df[BravesPitchDataToday_df['events'].isin(['single', 'double', 'triple', 'home_run'])].groupby('player_name').size().reset_index(name='hits')
    else:
        all_batters = BravesPitchDataToday_df['player_name'].unique()
        hits_df = pd.DataFrame({'player_name': all_batters, 'hits': 0})
    #walks
    if (BravesPitchDataToday_df['events'].isin(['walk', 'hit_by_pitch'])).any():
        walks_df = BravesPitchDataToday_df[BravesPitchDataToday_df['events'].isin(['walk', 'hit_by_pitch'])].groupby('player_name').size().reset_index(name='walks')
    else:
        all_batters = BravesPitchDataToday_df['player_name'].unique()
        walks_df = pd.DataFrame({'player_name': all_batters, 'walks': 0})
    #sacrifices
    if (BravesPitchDataToday_df['events'].isin(['sac_fly', 'sac_bunt'])).any():
        sacrifices_df = BravesPitchDataToday_df[BravesPitchDataToday_df['events'].isin(['sac_fly', 'sac_bunt'])].groupby('player_name').size().reset_index(name='sacrifices')
    else:
        all_batters = BravesPitchDataToday_df['player_name'].unique()
        sacrifices_df = pd.DataFrame({'player_name': all_batters, 'sacrifices': 0})
    #homeruns
    if (BravesPitchDataToday_df['events'].isin(['home_run'])).any():
        homeruns_df = BravesPitchDataToday_df[BravesPitchDataToday_df['events'].isin(['home_run'])].groupby('player_name').size().reset_index(name='home_runs')
    else:
        all_batters = BravesPitchDataToday_df['player_name'].unique()
        homeruns_df = pd.DataFrame({'player_name': all_batters, 'home_runs': 0})
    #calculate at bats
    #atbats_df['at_bats'] = atbats_df['at_bats'] - (walks_df['walks'] + sacrifices_df['sacrifices'])

    GameDate_df = BravesPitchDataToday_df[['player_name','game_date']].drop_duplicates(subset='player_name')
    BravesSummarizedDataToday_df = pd.merge(atbats_df, hits_df, on='player_name', how='left').fillna(0)
    BravesSummarizedDataToday_df = pd.merge(BravesSummarizedDataToday_df, walks_df, on='player_name', how='left').fillna(0)
    BravesSummarizedDataToday_df = pd.merge(BravesSummarizedDataToday_df, sacrifices_df, on='player_name', how='left').fillna(0)
    BravesSummarizedDataToday_df = pd.merge(BravesSummarizedDataToday_df, homeruns_df, on='player_name', how='left').fillna(0)
    BravesSummarizedDataToday_df = pd.merge(BravesSummarizedDataToday_df, GameDate_df, on='player_name', how='left')
    #set type to integer for all numbers
    BravesSummarizedDataToday_df[['hits', 'at_bats', 'walks', 'sacrifices', 'home_runs']] = BravesSummarizedDataToday_df[['hits', 'at_bats', 'walks', 'sacrifices', 'home_runs']].astype(int)
    #calculate at bats
    BravesSummarizedDataToday_df['at_bats'] = BravesSummarizedDataToday_df['at_bats'] - (BravesSummarizedDataToday_df['walks'] + BravesSummarizedDataToday_df['sacrifices'])
    #calculate average
    BravesSummarizedDataToday_df['average'] = round(BravesSummarizedDataToday_df['hits'] / BravesSummarizedDataToday_df['at_bats'], 3).fillna(0)
    BravesSummarizedDataToday_df['record'] = BravesSummarizedDataToday_df['hits'].astype(str) + '/' + BravesSummarizedDataToday_df['at_bats'].astype(str)
    #add todays date
    #BravesSummarizedDataToday_df['game_date'] = today

    BravesSummarizedDataNew_df = pd.concat([BravesSummarizedDataOld_df, BravesSummarizedDataToday_df], ignore_index=True)

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
