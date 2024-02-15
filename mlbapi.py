import pandas as pd
from pybaseball import  playerid_lookup


def player_lookup():
    player_data = playerid_lookup('albies', 'ozzie')

    player_data.to_csv('playerid_lookup.csv')
