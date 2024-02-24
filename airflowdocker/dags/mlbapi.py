

def player_lookup(**kwargs):
    import pandas as pd
    from datetime import datetime
    from pybaseball import  playerid_lookup
    from pybaseball import  statcast_batter

    ozziealbiesID = playerid_lookup('albies', 'ozzie')
    mattolsonID = playerid_lookup('olson', 'matt')
    austinrileyID = playerid_lookup('riley', 'austin')
    ronaldacunaID = playerid_lookup('acuña', 'ronald')

    ozziealbiesStats = statcast_batter('2023-07-01', '2023-07-21', player_id = int(ozziealbiesID['key_mlbam'].values[0]))
    mattolsonStats = statcast_batter('2023-07-01', '2023-07-21', player_id = int(mattolsonID['key_mlbam'].values[0]))
    austinrileyIDStats = statcast_batter('2023-07-01', '2023-07-21', player_id = int(austinrileyID['key_mlbam'].values[0]))
    ronaldacunaIDStats = statcast_batter('2023-07-01', '2023-07-21', player_id = int(ronaldacunaID['key_mlbam'].values[0]))

    bravesStats = pd.concat([ozziealbiesStats, mattolsonStats, austinrileyIDStats, ronaldacunaIDStats], ignore_index=True)
    
    bravesStats['events'].fillna('blank', inplace=True)
    bravesStats = bravesStats.loc[bravesStats['events'] != 'blank']
    print(f"Type of data: {type(bravesStats)}")

    timestamp = datetime.now().strftime('%Y%m%d')

    bravesStats_csv = 'bravesStats'+timestamp+'.csv'
    # Convert DataFrame to CSV string
    bravesStats.to_csv(bravesStats_csv, index=False)

    #return bravesStats
    kwargs['ti'].xcom_push(key='bravesStats_csv', value=bravesStats_csv)

def upload_bravesStats(**kwargs):
    from datetime import datetime
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bravesStats_csv = kwargs['ti'].xcom_pull(task_ids='player_lookup', key='bravesStats_csv')

    timestamp = datetime.now().strftime('%Y%m%d')
    bravesStats_csv = 'bravesStats'+timestamp+'.csv'

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='s3bucket')

    # Upload the CSV data to the specified S3 bucket and object (file) name
    s3_hook.load_file(
        filename =bravesStats_csv,
        key=bravesStats_csv,
        bucket_name='mlbdata1',
        replace=True,
    )

    return f'Successfully saved CSV to S3:mlbdata1/{bravesStats_csv}'


def battingstats(playerid):
    from pybaseball import  batting_stats_bref
    battingdata = batting_stats_bref(2023)

    battingdata.loc[battingdata['Tm'] == "Atlanta"]

    return battingdata


#bravesStats = player_lookup()
#upload_bravesStats(bravesStats, mlbdata1)