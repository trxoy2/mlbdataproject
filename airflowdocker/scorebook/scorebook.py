import turtle
import pandas as pd
from bases_grid import *
from scorebook_modules import *

BravesPitchData_df = pd.read_csv('C:/Users/troyb/Documents/github/mlbdataproject/airflowdocker/data/BravesPitchData.csv')
#Only show pitches where an event happened
BravesPitchData_df.fillna({'events': 'blank'}, inplace=True)
BravesPitchData_df = BravesPitchData_df.loc[BravesPitchData_df['events'] != 'blank']
BravesPitchData_df = BravesPitchData_df.drop_duplicates()


# set background image 
turtle.bgpic("C:/Users/troyb/Documents/github/mlbdataproject/airflowdocker/data/Scorebook.png") 
# Create a turtle object
s = turtle.Screen() 
t = turtle.Turtle()
t.pencolor('#808080')
t.fillcolor('#808080')
t.speed(10)
s.setup (width=1124, height=778, startx=0, starty=0)


Ronald_K_positions = ronald_K_positions()
Ozzie_K_positions = player_K_positions(Ronald_K_positions, "Ozzie")
Matt_K_positions = player_K_positions(Ozzie_K_positions, "Matt")
Austin_K_positions = player_K_positions(Matt_K_positions, "Austin")
Marcell_K_positions = player_K_positions(Austin_K_positions, "Marcell")
Michael_K_positions = player_K_positions(Marcell_K_positions, "Michael")
Sean_K_positions = player_K_positions(Michael_K_positions, "Sean")
Orlando_K_positions = player_K_positions(Sean_K_positions, "Orlando")
Jarred_K_positions = player_K_positions(Orlando_K_positions, "Jarred")

Ronald_walk_positions = ronald_walk_positions()
Ozzie_walk_positions = player_walk_positions(Ronald_walk_positions, "Ozzie")
Matt_walk_positions = player_walk_positions(Ozzie_walk_positions, "Matt")
Austin_walk_positions = player_walk_positions(Matt_walk_positions, "Austin")
Marcell_walk_positions = player_walk_positions(Austin_walk_positions, "Marcell")
Michael_walk_positions = player_walk_positions(Marcell_walk_positions, "Michael")
Sean_walk_positions = player_walk_positions(Michael_walk_positions, "Sean")
Orlando_walk_positions = player_walk_positions(Sean_walk_positions, "Orlando")
Jarred_walk_positions = player_walk_positions(Orlando_walk_positions, "Jarred")




batterids = [660670, 645277, 621566, 663586, 542303, 671739, 669221, 606115, 672284]
players = ['Ronald', 'Ozzie', 'Matt', 'Austin', 'Marcell', 'Michael', 'Sean', 'Orlando', 'Jarred']
# Fill out scroebook based on events in the DataFrame
for player, batterid in zip(players, batterids):
    K_positions = globals()[f"{player}_K_positions"]
    walk_positions = globals()[f"{player}_walk_positions"]
    for inning in range(1, 10):
        fill_out_scorebook(BravesPitchData_df, t, 8, '2023-07-15', inning, player, batterid, K_positions, walk_positions)




turtle.done()