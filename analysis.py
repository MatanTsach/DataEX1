import ingest as ing
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO)


def obtain_team_map():
    query = f"""
        SELECT * FROM team_map;
        """
    df = pd.DataFrame(list(session.execute(query)))

    return dict(zip(df['team_id'], df['team_name']))

def point_per_year(session):
    query = f"""
        SELECT season, avg_points FROM seasonal_performance;
        """
    
    # Fetching results
    result = session.execute(query)
    # Converting to dataframe
    df = pd.DataFrame(list(result))

    # Grouping by season
    df = df.groupby('season').mean().reset_index()
    # Sorting by season
    df_sorted = df.sort_values(by='season', ascending=True)
    print(df_sorted.head())

    # Create a bar graph
    bar_color = 'black'
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df_sorted['season'], df_sorted['avg_points'], color=bar_color)
    plt.xlabel('Season')
    plt.ylabel('Average Points')
    plt.title(f'Average Points per Season from 2003 to 2022')
    plt.xticks(df_sorted['season'], rotation=45)  # Rotate x-axis labels for readability
    plt.bar_label(bars, fmt='%d', label_type='edge', fontsize=9)
    plt.tight_layout()
    plt.show()

def seasonal_analysis(session, year, type='avg points', color='blue', ylabel='Average Points'):
    query = f"""
        SELECT team_id, {type}
        FROM seasonal_performance
        WHERE season = {year};
        """
    # Fetching results    
    results = session.execute(query)

    # Converting to dataframe
    df = pd.DataFrame(list(results))

    # Adding team names
    team_map = obtain_team_map()
    df['team_name'] = df['team_id'].apply(lambda x: team_map[x])

    # Sorting by average points
    df_sorted = df.sort_values(by=type, ascending=True)

    # Create a bar graph
    plt.figure(figsize=(12, 6))
    plt.bar_label(plt.bar(df_sorted['team_name'], df_sorted[type], color=color), fmt='%d', label_type='edge', fontsize=9)
    plt.xlabel('Team')
    plt.ylabel(ylabel)
    plt.title(f'{ylabel} per Team in {year}')
    plt.xticks(df_sorted['team_name'], rotation=90)
    y_extender = max(df_sorted[type]) * 1.15
    plt.ylim(0, y_extender)
    plt.tight_layout()
    plt.legend()
    plt.show()

def fetch_outcome_performance(session, season, outcome):
    query = f"""
        SELECT points, assists, rebounds, fg_pct, ft_pct, fg3_pct, outcome
        FROM game_outcome_performance
        WHERE season = {season} AND outcome = '{outcome}';
    """

    return list(session.execute(query))


def outcome_correlation(session, season):
    # Step 1: Fetch the data
    df_win = pd.DataFrame(fetch_outcome_performance(session, season, 'win'))
    df_loss = pd.DataFrame(fetch_outcome_performance(session, season, 'loss'))
    df = pd.concat([df_win, df_loss])
    # Step 2: Convert the outcome column to binary
    df['outcome'] = df['outcome'].apply(lambda x: 1 if x == 'win' else 0)
    # Step 3: Compute the correlation matrix
    correlation_matrix = df.corr()

    # Step 4: Create a heatmap with data labels
    fig, ax = plt.subplots(figsize=(10, 8))
    cax = ax.matshow(correlation_matrix, cmap='coolwarm')
    plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=90)
    plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
    plt.title(f'Correlation Heatmap in {season}')
    
    # Add colorbar
    plt.colorbar(cax)

    # Add text annotations
    for (i, j), val in np.ndenumerate(correlation_matrix):
        ax.text(j, i, f'{val:.2f}', ha='center', va='center', color='white')

    plt.show()

import matplotlib.pyplot as plt
from tabulate import tabulate

def top_players_by_average_points(session, top_n=5):
    print("Top players by points")
    query = """
            SELECT player_name, game_id, pts, reb, ast
            FROM player_stats
            """
    results = session.execute(query)
    df = pd.DataFrame(results._current_rows)

    query = """
            SELECT game_id FROM game_stats_outcome WHERE season = 2022 ALLOW FILTERING;
            """
    results = session.execute(query)
    df_games_id_from_2022 = pd.DataFrame(results._current_rows)
    
    players_sum_pts = {}
    players_sum_reb = {}
    players_sum_ast = {}
    players_count_games = {}

    for _, row in df.iterrows():
        if row['player_name'] not in players_sum_pts and row['game_id'] in df_games_id_from_2022['game_id'].values:
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

    print_top_players_tables(players_avg_pts[:top_n], players_avg_reb[:top_n], players_avg_ast[:top_n])

def print_top_players_tables(players_avg_pts, players_avg_reb, players_avg_ast):
    print("Top Players by Average Points:")
    print(tabulate(players_avg_pts, headers=["Player Name", "Average Points"], tablefmt="pretty"))
    
    print("Top Players by Average Rebounds:")
    print(tabulate(players_avg_reb, headers=["Player Name", "Average Rebounds"], tablefmt="pretty"))
    
    print("Top Players by Average Assists:")
    print(tabulate(players_avg_ast, headers=["Player Name", "Average Assists"], tablefmt="pretty"))

    # Create matplotlib tables
    fig, ax = plt.subplots(3, 1, figsize=(8, 10))

    table_pts = ax[0].table(cellText=players_avg_pts, colLabels=["Player Name", "Average Points"],
                            cellLoc="center", loc="center")
    table_pts.auto_set_font_size(False)
    table_pts.set_fontsize(10)
    table_pts.scale(1.2, 1.2)

    table_reb = ax[1].table(cellText=players_avg_reb, colLabels=["Player Name", "Average Rebounds"],
                            cellLoc="center", loc="center")
    table_reb.auto_set_font_size(False)
    table_reb.set_fontsize(10)
    table_reb.scale(1.2, 1.2)

    table_ast = ax[2].table(cellText=players_avg_ast, colLabels=["Player Name", "Average Assists"],
                            cellLoc="center", loc="center")
    table_ast.auto_set_font_size(False)
    table_ast.set_fontsize(10)
    table_ast.scale(1.2, 1.2)

    # Remove axis
    for ax_table in ax:
        ax_table.axis("off")

    plt.show()

def get_deni_avdija_stats(session):
    query = """
            SELECT game_id FROM game_stats_outcome WHERE season = 2022 ALLOW FILTERING;
            """
    results = session.execute(query)
    df_games_2022 = pd.DataFrame(results._current_rows)

    query = """
            SELECT game_id FROM game_stats_outcome WHERE season = 2021 ALLOW FILTERING;
            """
    results = session.execute(query)
    df_games_2021 = pd.DataFrame(results._current_rows)

    query = """
            SELECT game_id FROM game_stats_outcome WHERE season = 2020 ALLOW FILTERING;
            """
    results = session.execute(query)
    df_games_2020 = pd.DataFrame(results._current_rows)

    query = """
            SELECT player_name, game_id, pts, reb, ast
            FROM player_stats
            WHERE player_name = 'Deni Avdija';
            """
    results = session.execute(query)
    df_deni_avdija_stats = pd.DataFrame(results._current_rows)

    deni_pts_2022 = 0
    deni_pts_2021 = 0
    deni_pts_2020 = 0
    deni_reb_2022 = 0
    deni_reb_2021 = 0
    deni_reb_2020 = 0
    deni_ast_2022 = 0
    deni_ast_2021 = 0
    deni_ast_2020 = 0
    deni_games_2022 = 0
    deni_games_2021 = 0
    deni_games_2020 = 0

    deni_pts_per_game_2022 = 0

    for _, row in df_deni_avdija_stats.iterrows():
        if row['game_id'] in df_games_2022['game_id'].values:
            deni_pts_2022 += row['pts']
            deni_reb_2022 += row['reb']
            deni_ast_2022 += row['ast']
            deni_games_2022 += 1
        if row['game_id'] in df_games_2021['game_id'].values:
            deni_pts_2021 += row['pts']   
            deni_reb_2021 += row['reb']
            deni_ast_2021 += row['ast']
            deni_games_2021 += 1
        if row['game_id'] in df_games_2020['game_id'].values:
            deni_pts_2020 += row['pts']
            deni_reb_2020 += row['reb']
            deni_ast_2020 += row['ast']
            deni_games_2020 += 1

    # Calculate points per game for each season
    deni_pts_per_game_2022 = deni_pts_2022 / deni_games_2022 if deni_games_2022 != 0 else 0
    deni_pts_per_game_2021 = deni_pts_2021 / deni_games_2021 if deni_games_2021 != 0 else 0
    deni_pts_per_game_2020 = deni_pts_2020 / deni_games_2020 if deni_games_2020 != 0 else 0

    return deni_pts_per_game_2022, deni_pts_per_game_2021, deni_pts_per_game_2020, \
           deni_reb_2022 / deni_games_2022 if deni_games_2022 != 0 else 0, \
           deni_reb_2021 / deni_games_2021 if deni_games_2021 != 0 else 0, \
           deni_reb_2020 / deni_games_2020 if deni_games_2020 != 0 else 0, \
           deni_ast_2022 / deni_games_2022 if deni_games_2022 != 0 else 0, \
           deni_ast_2021 / deni_games_2021 if deni_games_2021 != 0 else 0, \
           deni_ast_2020 / deni_games_2020 if deni_games_2020 != 0 else 0

def plot_deni_avdija_stats(stats):
    labels = ['2022', '2021', '2020']
    points_per_game = stats[:3]
    rebounds_per_game = stats[3:6]
    assists_per_game = stats[6:]

    # Plotting
    fig, axes = plt.subplots(3, 1, figsize=(10, 12))

    axes[0].bar(labels, points_per_game, color='blue', alpha=0.7)
    axes[0].set_title('Points per Game')
    axes[0].set_ylabel('Points')

    axes[1].bar(labels, rebounds_per_game, color='green', alpha=0.7)
    axes[1].set_title('Rebounds per Game')
    axes[1].set_ylabel('Rebounds')

    axes[2].bar(labels, assists_per_game, color='orange', alpha=0.7)
    axes[2].set_title('Assists per Game')
    axes[2].set_ylabel('Assists')

    plt.tight_layout()
    plt.show()

    
if __name__ == "__main__":
    cluster, session = ing.connect_cassandra()
    #ing.ingest()
    # ing.ingest_game_outcome_performance(session, pd.read_csv('raw/games.csv'))
    #point_per_year(session)
    #top_players_by_average_points(session, 5)
    deni_stats = get_deni_avdija_stats(session)
    plot_deni_avdija_stats(deni_stats)
    # seasonal_analysis(session, 2022, 'avg_points', 'green', 'Average Points')
    # outcome_correlation(session, 2022)
    # Close connection
    session.shutdown()
    cluster.shutdown()
