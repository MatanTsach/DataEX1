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



if __name__ == "__main__":
    cluster, session = ing.ingest()
    logging.info("Ingestion complete")
    win_margin(session, 'Home')
    # Close connection
    session.shutdown()
    cluster.shutdown()
