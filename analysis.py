import ingest as ing
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO)


def win_margin(session, game_type='Home'):
    query = f"""
                SELECT team_id, team_points, opponent_points
                FROM game_stats_outcome
                WHERE season = 2022
                """
    results = session.execute(query)

    df = pd.DataFrame(results._current_rows)
    df['win_margin'] = df['team_points'] - df['avg_win_margin']
    avg_win_margin = df.groupby('team_id')['win_margin'].mean()
    print(avg_win_margin.head())


def top_players_by_average_points(session, top_n=5):
    print("Top players by points")
    query = """
            SELECT player_name, pts, reb, ast
            FROM player_stats
            """
    results = session.execute(query)
    df = pd.DataFrame(results._current_rows)
    
    players_sum_pts = {}
    players_sum_reb = {}
    players_sum_ast = {}
    players_count_games = {}

    for _, row in df.iterrows():
        if row['player_name'] not in players_sum_pts:
            players_sum_pts[row['player_name']] = row['pts']
            players_sum_reb[row['player_name']] = row['reb']
            players_sum_ast[row['player_name']] = row['ast']
            players_count_games[row['player_name']] = 1
        else:
            players_sum_pts[row['player_name']] += row['pts']
            players_sum_reb[row['player_name']] += row['reb']
            players_sum_ast[row['player_name']] += row['ast']
            players_count_games[row['player_name']] += 1

    players_avg_pts = {}
    for player in players_sum_pts:
        players_avg_pts[player] = players_sum_pts[player] / players_count_games[player]

    players_avg_pts = sorted(players_avg_pts.items(), key=lambda x: x[1], reverse=True)

    players_avg_reb = {}
    for player in players_sum_reb:
        players_avg_reb[player] = players_sum_reb[player] / players_count_games[player]

    players_avg_reb = sorted(players_avg_reb.items(), key=lambda x: x[1], reverse=True)

    players_avg_ast = {}    
    for player in players_sum_ast:
        players_avg_ast[player] = players_sum_ast[player] / players_count_games[player]

    players_avg_ast = sorted(players_avg_ast.items(), key=lambda x: x[1], reverse=True)    

    print(players_avg_pts[:top_n])
    print(players_avg_reb[:top_n])
    print(players_avg_ast[:top_n])
    
if __name__ == "__main__":
    cluster, session = ing.ingest()
    logging.info("Ingestion complete")
    #win_margin(session, 'Home')
    
    # Add the new function call
    top_players_by_average_points(session, top_n=5)
    
    # Close connection
    session.shutdown()
    cluster.shutdown()
