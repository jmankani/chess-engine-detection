import chess
import chess.pgn
import chess.engine
import pandas as pd

# Create empty DataFrames to store the gamevise and playerwise engine move percentages
gamewise_engine_move_percentages = pd.DataFrame(columns=["Game", "White Player", "Black Player" , "White Engine Move Percentage", "Black Engine Move Percentage"])


def analyze_game(pgn_path):
    """Analyzes a game from a PGN file using Stockfish engine.

    Args:
        pgn_path (str): The path to the PGN file.
    """
    # Load a game from a PGN file
    with open(pgn_path) as pgn:
        while True:
            game = chess.pgn.read_game(pgn)

            if game is None:
                break
            board = game.board()
            engine_path = "stockfish\stockfish-windows-x86-64-avx2.exe"
            with chess.engine.SimpleEngine.popen_uci(engine_path) as engine:
                white_engine_move_count = 0
                black_engine_move_count = 0
                for move in game.mainline_moves():
                    # Analyze the position after the move
                    result = engine.analyse(board, chess.engine.Limit(time=0.1))

                    # Extract the best move from the analysis and give a quality score 1 if the move matches the engine's best move
                    best_move = result['pv'][0]
                    move_quality = 1 if move == best_move else 0

                    # Increment the engine move count for white and black
                    if board.turn == chess.WHITE:
                        white_engine_move_count += move_quality
                    else:
                        black_engine_move_count += move_quality
                    board.push(move)

                # Print engine move count percentage for white and black
                white_engine_move_percentage = white_engine_move_count / game.end().board().fullmove_number
                black_engine_move_percentage = black_engine_move_count / game.end().board().fullmove_number

                # Append the engine move percentages to the gamewise_engine_move_percentages DataFrame
                gamewise_engine_move_percentages.loc[len(gamewise_engine_move_percentages)] = [pgn_path, game.headers["White"], game.headers["Black"], white_engine_move_percentage, black_engine_move_percentage]
        
        print(gamewise_engine_move_percentages)



# Analyze a game from a PGN file
analyze_game("pgn_repo\chess_com_games_2024-06-15.pgn")




