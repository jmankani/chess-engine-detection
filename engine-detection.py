import chess
import chess.pgn
import chess.engine
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import pickle
import time

sys.setrecursionlimit(30000)

PGN_PATH = "D:\lichess_db_standard_rated_2024-05.pgn"
ENGINE_PATH = engine_path = "stockfish\stockfish-windows-x86-64-avx2.exe"
NUM_WORKERS = os.cpu_count()
CACHE_PATH = 'engine_cache.pkl'

# Load or initialize the engine cache
try:
    with open(CACHE_PATH, 'rb') as f:
        engine_cache = pickle.load(f)

except FileNotFoundError:
    engine_cache = {}

# Create empty DataFrames to store the gamevise data
gamewise_engine_move_percentages = pd.DataFrame(columns=["game","date", "time_control", "white_id", "white_elo", "white_engine_move_percentage","black_id", "black_elo", "black_engine_move_percentage", "result"])

def evaluate_board(engine, board, cache_results=True):
    """Evaluate the board using the chess engine and cache results."""
    board_fen = board.fen()
    if board_fen in engine_cache:
        return engine_cache[board_fen]
    result = engine.analyse(board, chess.engine.Limit(depth=10))
    if cache_results:
        engine_cache[board_fen] = result['pv'][0]
    return result['pv'][0]

def analyze_game(game):
    """Analyze a single game and compute statistics to assess engine use."""
    board = game.board()

    white_engine_move_count = 0
    black_engine_move_count = 0
    with chess.engine.SimpleEngine.popen_uci(ENGINE_PATH) as engine:
        for move in game.mainline_moves():

            # Cache results only if first 15 moves
            best_move = evaluate_board(engine, board, cache_results=board.fullmove_number <= 20)
            move_quality = 1 if move == best_move else 0

            # Increment the engine move count for white and black
            if board.turn == chess.WHITE:
                white_engine_move_count += move_quality
            else:
                black_engine_move_count += move_quality
            board.push(move)
    
    # Calculate engine move count percentage for white and black
    white_engine_move_percentage = white_engine_move_count / game.end().board().fullmove_number
    black_engine_move_percentage = black_engine_move_count / game.end().board().fullmove_number

    return game, game.headers["Date"], game.headers["TimeControl"] ,game.headers["White"], \
        game.headers["WhiteElo"], white_engine_move_percentage, game.headers["Black"], game.headers["BlackElo"], black_engine_move_percentage, game.headers["Result"]


def game_generator(file_path):
    """Generator to yield games from a PGN file."""
    with open(file_path) as pgn:
        # while True:
        for _ in range(5000):
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            yield game

def get_user_data_from_game_data(df):
    """
    Create a dictionary of dataframes, each dataframe filtered for games played by a specific player
    and formatted to show user-specific details including the result from the player's perspective.
    
    Parameters:
        df (pd.DataFrame): The original dataframe containing chess games.
        
    Returns:
        dict: A dictionary where keys are player IDs and values are the user-specific dataframes.
    """
    # Prepare empty DataFrame to collect all player-specific rows
    all_games_user_perspective = pd.DataFrame()

    # Create a set of all unique player IDs from both 'white_id' and 'black_id'
    all_players = pd.concat([df['white_id'], df['black_id']]).unique()

    for player_id in all_players:
        # Filter games for the current player as white
        games_as_white = df[df['white_id'] == player_id].copy()
        games_as_white['user'] = player_id
        games_as_white['elo'] = games_as_white['white_elo']
        games_as_white['engine_move_percent'] = games_as_white['white_engine_move_percentage']
        games_as_white['user_result'] = games_as_white['result'].map({'1-0': 'Win', '0-1': 'Loss', '1/2-1/2': 'Draw'})

        # Filter games for the current player as black
        games_as_black = df[df['black_id'] == player_id].copy()
        games_as_black['user'] = player_id
        games_as_black['elo'] = games_as_black['black_elo']
        games_as_black['engine_move_percent'] = games_as_black['black_engine_move_percentage']
        games_as_black['user_result'] = games_as_black['result'].map({'1-0': 'Loss', '0-1': 'Win', '1/2-1/2': 'Draw'})

        # Combine both DataFrames
        user_games = pd.concat([games_as_white, games_as_black], ignore_index=True)

        # Select only the necessary columns to match the desired output format
        user_specific_df = user_games[['date', 'user', 'elo', 'time_control', 'engine_move_percent', 'user_result']]

        # Append to the final DataFrame
        all_games_user_perspective = pd.concat([all_games_user_perspective, user_specific_df], ignore_index=True)
    
    return all_games_user_perspective

def main():
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for result in executor.map(analyze_game, game_generator(PGN_PATH)):
            gamewise_engine_move_percentages.loc[len(gamewise_engine_move_percentages)] = result

    # Save the updated engine cache
    with open(CACHE_PATH, 'wb') as f:
        pickle.dump(engine_cache, f)
    
    print("--- %s seconds ---" % (time.time() - start_time))

    userwise_game_data = get_user_data_from_game_data(gamewise_engine_move_percentages)
    # Save both DataFrames to CSV files
    userwise_game_data.to_csv('userwise_game_data.csv', index=False)
    gamewise_engine_move_percentages.to_csv('gamewise_engine_move_percentages.csv', index=False)

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    main()
