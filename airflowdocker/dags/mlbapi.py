

def player_lookup():
    import pandas as pd
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
    print(bravesStats)

    return bravesStats

def battingstats(playerid):
    from pybaseball import  batting_stats_bref
    battingdata = batting_stats_bref(2023)

    battingdata.loc[battingdata['Tm'] == "Atlanta"]

    return battingdata


player_lookup()