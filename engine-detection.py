import chess
import chess.pgn
import chess.engine

def analyze_game(pgn_path):
    """Analyzes a game from a PGN file using Stockfish engine.

    Args:
        pgn_path (str): The path to the PGN file.
    """
    # Load a game from a PGN file
    with open(pgn_path) as pgn:
        game = chess.pgn.read_game(pgn)

    board = game.board()
    engine_path = "stockfish\stockfish-windows-x86-64-avx2.exe"
    with chess.engine.SimpleEngine.popen_uci(engine_path) as engine:
        white_engine_move_count = 0
        black_engine_move_count = 0
        for move in game.mainline_moves():
            # Analyze the position after the move
            result = engine.analyse(board, chess.engine.Limit(time=10))

            # Extract the best move from the analysis and give a quality score 1 if the move matches the engine's best move
            best_move = result['pv'][0]
            move_quality = 1 if move == best_move else 0

            # Increment the engine move count for white and black
            if board.turn == chess.WHITE:
                white_engine_move_count += move_quality
            else:
                black_engine_move_count += move_quality
            print(f"Move: {move}, Best Move: {best_move}, Match: {move_quality}")
            board.push(move)

        # Print engine move count percentage for white and black
        white_engine_move_percentage = white_engine_move_count / game.end().board().fullmove_number
        black_engine_move_percentage = black_engine_move_count / game.end().board().fullmove_number
        print(f"White Engine Move Percentage: {white_engine_move_percentage}")
        print(f"Black Engine Move Percentage: {black_engine_move_percentage}")

# Analyze a game from a PGN file
analyze_game("pgn_repo\chess_com_games_2024-06-15.pgn")



