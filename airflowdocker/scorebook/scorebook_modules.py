from bases_grid import *

inning_column_offset = 73.8
player_row_offset = 57

def ronald_K_positions():
    initial_position = (-317, 243)
    positions = [initial_position]
    num_innings = 10
    positions_dict = {"Ronald{}InningK".format(inning): position for inning, position in enumerate(positions, start=1)}
    for inning in range(2, num_innings + 1):
        new_position = (positions[-1][0] + inning_column_offset, initial_position[1])
        positions.append(new_position)
        positions_dict["Ronald{}InningK".format(inning)] = new_position
    return positions_dict

def player_K_positions(previous_player_K_positions, player_name):
    initial_position = list(previous_player_K_positions.values())[0]  # Get the first position in the dictionary
    initial_position = (initial_position[0], initial_position[1] - player_row_offset)  # Adjust the position
    positions = [initial_position]
    num_innings = 10
    positions_dict = {f"{player_name}{{}}InningK".format(inning): position for inning, position in enumerate(positions, start=1)}
    for inning in range(2, num_innings + 1):
        new_position = (positions[-1][0] + inning_column_offset, initial_position[1])
        positions.append(new_position)
        positions_dict[f"{player_name}{{}}InningK".format(inning)] = new_position
    return positions_dict

def ronald_walk_positions():
    initial_position = (-300, 245)
    positions = [initial_position]
    num_innings = 10
    positions_dict = {"Ronald{}Inningwalk".format(inning): position for inning, position in enumerate(positions, start=1)}
    for inning in range(2, num_innings + 1):
        new_position = (positions[-1][0] + inning_column_offset, initial_position[1])
        positions.append(new_position)
        positions_dict["Ronald{}Inningwalk".format(inning)] = new_position
    return positions_dict

def player_walk_positions(previous_player_walk_positions, player_name):
    initial_position = list(previous_player_walk_positions.values())[0]  # Get the first position in the dictionary
    initial_position = (initial_position[0], initial_position[1] - player_row_offset)  # Adjust the position
    positions = [initial_position]
    num_innings = 10
    positions_dict = {f"{player_name}{{}}Inningwalk".format(inning): position for inning, position in enumerate(positions, start=1)}
    for inning in range(2, num_innings + 1):
        new_position = (positions[-1][0] + inning_column_offset, initial_position[1])
        positions.append(new_position)
        positions_dict[f"{player_name}{{}}Inningwalk".format(inning)] = new_position
    return positions_dict


def fill_out_scorebook(df, t, size, game_date, inning, player, batterid, K_positions, walk_positions):
    df = df[(df['game_date'] == game_date) & (df['inning'] == inning) & (df['batter'] == batterid)]
    
    first_base_name = f'{player}{inning}InningFirstBase'
    second_base_name = f'{player}{inning}InningSecondBase'
    third_base_name = f'{player}{inning}InningThirdBase'
    home_plate_name = f'{player}{inning}InningHomePlate'

    #K_name = f'{player}_K_positions'

    for index, row in df.iterrows():
        if row['events'] == 'single':
            draw_base(t, globals()[first_base_name], size)
        elif row['events'] == 'double':
            draw_base(t, globals()[first_base_name], size)
            draw_base(t, globals()[second_base_name], size)
        elif row['events'] == 'triple':
            draw_base(t, globals()[first_base_name], size)
            draw_base(t, globals()[second_base_name], size)
            draw_base(t, globals()[third_base_name], size)
        elif row['events'] == 'home_run':
            draw_base(t, globals()[first_base_name], size)
            draw_base(t, globals()[second_base_name], size)
            draw_base(t, globals()[third_base_name], size)
            draw_base(t, globals()[home_plate_name], size)
        elif row['events'] == 'strikeout' and row['description'] == 'called_strike':
            for var_name, position in K_positions.items():
                if var_name == f'{player}{inning}InningK':
                    draw_backwards_K(t, position)
        elif row['events'] == 'strikeout' and row['description'] != 'called_strike':
            for var_name, position in K_positions.items():
                if var_name == f'{player}{inning}InningK':
                    draw_K(t, position)
        elif row['events'] == 'walk' or row['events'] == 'hit_by_pitch':
            draw_base(t, globals()[first_base_name], size)
            for var_name, position in walk_positions.items():
                if var_name == f'{player}{inning}Inningwalk':
                    draw_walk(t, position)


def draw_base(t, position, size):
    t.penup()
    t.goto(position)
    t.setheading(45)  # Rotate by 45 degrees
    t.pendown()
    t.pencolor('#808080')
    t.fillcolor('#808080')
    t.pensize(4)
    t.begin_fill()
    for _ in range(4):
        t.forward(size)
        t.right(90)
    t.end_fill()


def draw_K(t, position):
    # Draw the letter 'K'
    t.penup()
    t.goto(position)
    t.setheading(0)
    t.pendown()
    t.pensize(5)
    t.pencolor("#D85A5E")
    t.left(90)
    t.forward(30)
    t.backward(15)
    t.right(45)
    t.forward(21)
    t.backward(21)
    t.right(90)
    t.forward(21)
    t.left(45)

def draw_backwards_K(t, position):
    # Draw the letter 'K'
    t.penup()
    t.goto(position)
    t.setheading(0)
    t.pendown()
    t.pensize(5)
    t.pencolor("#D85A5E")
    t.left(90)
    t.forward(30)
    t.backward(15)
    t.left(45)
    t.forward(21)
    t.backward(21)
    t.left(90)
    t.forward(21)
    t.right(45)

def draw_walk(t, position):
    # Draw the letter 'K'
    t.penup()
    t.goto(position)
    t.setheading(0)
    t.pendown()
    t.pensize(3)
    t.pencolor("#4887D4")
    t.right(80)
    t.forward(13)
    t.right(-140)
    t.forward(9)
    t.right(120)
    t.forward(9)
    t.left(140)
    t.forward(13)