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
gamewise_engine_move_percentages = pd.DataFrame(columns=["game", "white_id", "black_id" , "white_engine_move_percentage", "black_engine_move_percentage"])

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
            best_move = evaluate_board(engine, board, cache_results=board.fullmove_number <= 15)
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

    return game, game.headers["White"], game.headers["Black"], white_engine_move_percentage, black_engine_move_percentage


def game_generator(file_path):
    """Generator to yield games from a PGN file."""
    with open(file_path) as pgn:
        # while True:
        for _ in range(1500):
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            yield game

def main():
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for result in executor.map(analyze_game, game_generator(PGN_PATH)):
            gamewise_engine_move_percentages.loc[len(gamewise_engine_move_percentages)] = result

    # Save the updated engine cache
    with open(CACHE_PATH, 'wb') as f:
        pickle.dump(engine_cache, f)

    print(gamewise_engine_move_percentages)
    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    main()
