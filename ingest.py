import pandas as pd
from cassandra.cluster import Cluster
import logging
import configparser


# Configuration
config = configparser.ConfigParser()
config.read('config.ini')  # Configuration file

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_cassandra():
    cluster = Cluster([config['CASSANDRA']['HOST']], port=config['CASSANDRA']['PORT'])
    session = cluster.connect()
    session.set_keyspace(config['CASSANDRA']['KEYSPACE'])
    return cluster, session

def execute_cql_scripts(session, cql_script):
    """Execute CQL commands from a .cql file."""
    with open(cql_script, 'r') as file:
        cql_commands = file.read()
        for command in cql_commands.split(';'):
            command = command.strip()  # Remove leading/trailing whitespace
            if command:
                logger.info(f"Executing CQL command:\n{command}")
                session.execute(command)


def ingest_game_stats(session, games_df):
    logging.info('Starting to ingest game stats')
    games_df['home_outcome'] = games_df['HOME_TEAM_WINS'].apply(lambda x: 'win' if x == 1 else 'loss')
    games_df['away_outcome'] = games_df['HOME_TEAM_WINS'].apply(lambda x: 'loss' if x == 1 else 'win')

    prepared = session.prepare("""
        INSERT INTO game_stats_outcome (
            game_id,
            team_id,
            opponent_team_id,
            season,
            game_type,
            team_points,
            opponent_points,
            team_ast,
            team_reb,
            team_fg_pct,
            team_fg3_pct,
            game_outcome
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for _, row in games_df.iterrows():
        if pd.isna(row['PTS_home']):
            continue
        # Insert row for the home team
        session.execute(prepared, (
            row['GAME_ID'],
            row['HOME_TEAM_ID'],
            row['VISITOR_TEAM_ID'],
            row['SEASON'],
            "Home",
            int(row['PTS_home']),
            int(row['PTS_away']),
            int(row['AST_home']),
            int(row['REB_home']),
            row['FG_PCT_home'],
            row['FG3_PCT_home'],
            row['home_outcome']
        ))
        
        # Insert row for the away team
        session.execute(prepared, (
            row['GAME_ID'],
            row['VISITOR_TEAM_ID'],
            row['HOME_TEAM_ID'],
            row['SEASON'],
            "Away",
            int(row['PTS_away']),
            int(row['PTS_home']),
            int(row['AST_away']),
            int(row['REB_away']),
            row['FG_PCT_away'],
            row['FG3_PCT_away'],
            row['away_outcome']
        ))

    logging.info('Finished ingesting game stats')

def ingest_seasonal_performance(session, games_df):
    logging.info('Starting to ingest seasonal performance')
    home_averages = games_df.groupby(['HOME_TEAM_ID', 'SEASON']).agg({
        'PTS_home': 'mean',
        'REB_home': 'mean',
        'AST_home': 'mean'
    }).reset_index().rename(columns={
        'HOME_TEAM_ID': 'team_id',
        'PTS_home': 'avg_points',
        'REB_home': 'avg_rebounds',
        'AST_home': 'avg_assists'
    })

    # Calculate averages for away games
    away_averages = games_df.groupby(['VISITOR_TEAM_ID', 'SEASON']).agg({
        'PTS_away': 'mean',
        'REB_away': 'mean',
        'AST_away': 'mean'
    }).reset_index().rename(columns={
        'VISITOR_TEAM_ID': 'team_id',
        'PTS_away': 'avg_points',
        'REB_away': 'avg_rebounds',
        'AST_away': 'avg_assists'
    })

    seasonal_averages = pd.concat([home_averages, away_averages])

    # Calculate overall average per team and season
    seasonal_averages = seasonal_averages.groupby(['team_id', 'SEASON']).mean().reset_index()

    prepared = session.prepare("""
        INSERT INTO seasonal_performance (
            team_id,
            season,
            avg_points,
            avg_rebounds,
            avg_assists
        ) VALUES (?, ?, ?, ?, ?)
    """)

    for index, row in seasonal_averages.iterrows():
        session.execute(prepared, (
            int(row['team_id']),
            int(row['SEASON']),
            row['avg_points'],
            row['avg_rebounds'],
            row['avg_assists']
        ))
    
    logging.info('Finished ingesting seasonal performance')


if __name__ == "__main__":
    cluster, session  = connect_cassandra()
    
    execute_cql_scripts(session, config['CQL']['SCHEMA'])
    games_df = pd.read_csv(config['DATA']['GAMES_CSV'])
    ingest_game_stats(session, games_df)
    ingest_seasonal_performance(session, games_df)