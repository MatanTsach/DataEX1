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

if __name__ == "__main__":
    cluster, session = ing.connect_cassandra()
    # ing.ingest_game_outcome_performance(session, pd.read_csv('raw/games.csv'))
    point_per_year(session)
    # seasonal_analysis(session, 2022, 'avg_points', 'green', 'Average Points')
    # outcome_correlation(session, 2022)
    # Close connection
    session.shutdown()
    cluster.shutdown()
