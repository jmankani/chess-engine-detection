import chess
import chess.pgn
import chess.engine
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import os
import sys

sys.setrecursionlimit(30000)

PGN_PATH = "pgn_repo\chess_com_games_2024-06-15.pgn"
ENGINE_PATH = engine_path = "stockfish\stockfish-windows-x86-64-avx2.exe"
NUM_WORKERS = os.cpu_count()

# Create empty DataFrames to store the gamevise data
gamewise_engine_move_percentages = pd.DataFrame(columns=["game", "white_id", "black_id" , "white_engine_move_percentage", "black_engine_move_percentage"])

def evaluate_board(engine, board):
    """Evaluate the board using the chess engine and cache results."""
    result = engine.analyse(board, chess.engine.Limit(time=0.1))
    return result['pv'][0]

def analyze_game(game):
    """Analyze a single game and compute statistics to assess engine use."""
    board = game.board()
    white_engine_move_count = 0
    black_engine_move_count = 0
    with chess.engine.SimpleEngine.popen_uci(ENGINE_PATH) as engine:
        for move in game.mainline_moves():
            best_move = evaluate_board(engine, board)
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
        while True:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            yield game

def main():
    results = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for game, white_id, black_id, white_engine_move_percentage, black_engine_move_percentage in executor.map(analyze_game, game_generator(PGN_PATH)):
            # Append the engine move percentages to the gamewise_engine_move_percentages DataFrame
            gamewise_engine_move_percentages.loc[len(gamewise_engine_move_percentages)] = [game, white_id, black_id, white_engine_move_percentage, black_engine_move_percentage]

    print(gamewise_engine_move_percentages)

if __name__ == "__main__":
    main()




