import chess
import chess.pgn
import chess.engine
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
import pickle
import time
import logging

# Create folder to save logs
os.makedirs('logs', exist_ok=True)

# Create string with current date and time
current_time = time.strftime("%Y-%m-%d-%H-%M-%S")

# Set log file path
log_file = os.path.join('logs', f'{current_time}_engine-detection.log')

# Set up logging to file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file)

# Configuration
PGN_PATH = os.path.join("D:", "lichess_db_standard_rated_2024-05.pgn")
ENGINE_PATH = os.path.join("stockfish", "stockfish-windows-x86-64-avx2.exe")
NUM_WORKERS = os.cpu_count()
CACHE_PATH = 'engine_cache.pkl'
GAME_COUNT = 50000

# Load or initialize the engine cache
try:
    with open(CACHE_PATH, 'rb') as f:
        engine_cache = pickle.load(f)
except FileNotFoundError:
    engine_cache = {}
except Exception as e:
    logging.error(f"Error loading cache: {e}")
    engine_cache = {}

# Create empty DataFrame to store the game-wise data
gamewise_engine_move_percentages = pd.DataFrame(columns=["game", "date", "time_control", "white_id", "white_elo", "white_engine_move_percentage", "black_id", "black_elo", "black_engine_move_percentage", "result"])

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

            best_move = evaluate_board(engine, board, cache_results=board.fullmove_number <= 20)
            move_quality = 1 if move == best_move else 0

            if board.turn == chess.WHITE:
                white_engine_move_count += move_quality
            else:
                black_engine_move_count += move_quality
            board.push(move)

    total_moves = board.fullmove_number
    white_engine_move_percentage = white_engine_move_count / total_moves if total_moves else 0
    black_engine_move_percentage = black_engine_move_count / total_moves if total_moves else 0

    return game, game.headers["Date"], game.headers["TimeControl"], game.headers["White"], \
        game.headers["WhiteElo"], white_engine_move_percentage, game.headers["Black"], \
        game.headers["BlackElo"], black_engine_move_percentage, game.headers["Result"]

def game_generator(file_path):
    """Generator to yield games from a PGN file."""
    logging.info(f"Analyzing {GAME_COUNT} games...")
    with open(file_path) as pgn:
        for _ in range(GAME_COUNT):
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            yield game

def get_user_data_from_game_data(df):
    """ Create a dictionary of dataframes for each player's perspective in games. """
    all_games_user_perspective = pd.DataFrame()
    all_players = pd.concat([df['white_id'], df['black_id']]).unique()

    for player_id in all_players:
        user_games = pd.concat([
            df.query('white_id == @player_id').assign(user=player_id, elo=df['white_elo'], engine_move_percent=df['white_engine_move_percentage'], user_result=df['result'].map({'1-0': 'Win', '0-1': 'Loss', '1/2-1/2': 'Draw'})),
            df.query('black_id == @player_id').assign(user=player_id, elo=df['black_elo'], engine_move_percent=df['black_engine_move_percentage'], user_result=df['result'].map({'1-0': 'Loss', '0-1': 'Win', '1/2-1/2': 'Draw'}))
        ], ignore_index=True)
        all_games_user_perspective = pd.concat([all_games_user_perspective, user_games[['date', 'user', 'elo', 'time_control', 'engine_move_percent', 'user_result']]], ignore_index=True)

    return all_games_user_perspective

def main():
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for result in executor.map(analyze_game, game_generator(PGN_PATH)):
            gamewise_engine_move_percentages.loc[len(gamewise_engine_move_percentages)] = result

    with open(CACHE_PATH, 'wb') as f:
        pickle.dump(engine_cache, f)

    userwise_game_data = get_user_data_from_game_data(gamewise_engine_move_percentages)

    os.makedirs('results', exist_ok=True)
    userwise_game_data.to_csv(f'results/{current_time}_userwise_game_data.csv', index=False)
    gamewise_engine_move_percentages.to_csv(f'results/{current_time}_gamewise_engine_move_percentages.csv', index=False)

    logging.info(f"--- {time.time() - start_time} seconds ---")

if __name__ == "__main__":
    main()
