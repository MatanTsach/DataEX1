from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import pandas as pd

HOST = '127.0.0.1'
PORT = 9042
KEYSPACE = 'nba_data'
CQL_SCRIPT = 'cql/games.cql'

cluster = Cluster([HOST], port=PORT)
session = cluster.connect()

with open(CQL_SCRIPT, 'r') as file:
    cql_commands = file.read()

    for command in cql_commands.split(';'):
        command = command.strip()  # Remove leading/trailing whitespace
        if command:
            print(f"Executing CQL command:\n{command}")
            session.execute(command)

session.set_keyspace(KEYSPACE)

games_df = pd.read_csv('raw/games.csv')
insert_stmt = session.prepare("""
INSERT INTO games (
    team_id,
    game_type,
    season,
    rival_team_id,
    game_id,
    team_pts,
    team_pg_pct,
    team_ft_pct,
    team_fg3_pct,
    team_ast,
    team_reb,
    rival_pts,
    rival_pg_pct,
    rival_ft_pct,
    rival_ast_pct,
    rival_ast,
    rival_reb
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

batch = BatchStatement()
for _, row in games_df.iterrows():
    if pd.isna(row['PTS_home']):
        continue

    home_bound_stmt = insert_stmt.bind((
        row['HOME_TEAM_ID'],
        'HOME',
        row['SEASON'],
        row['VISITOR_TEAM_ID'],
        row['GAME_ID'],
        int(row['PTS_home']),
        row['FG_PCT_home'],
        row['FT_PCT_home'],
        row['FG3_PCT_home'],
        int(row['AST_home']),
        int(row['REB_home']),
        int(row['PTS_away']),
        row['FG_PCT_away'],
        row['FT_PCT_away'],
        row['FG3_PCT_away'],
        int(row['AST_away']),
        int(row['REB_away'])
    ))

    away_bound_stmt = insert_stmt.bind((
        row['VISITOR_TEAM_ID'],
        'AWAY',
        row['SEASON'],
        row['HOME_TEAM_ID'],
        row['GAME_ID'],
        int(row['PTS_away']),
        row['FG_PCT_away'],
        row['FT_PCT_away'],
        row['FG3_PCT_away'],
        int(row['AST_away']),
        int(row['REB_away']),
        int(row['PTS_home']),
        row['FG_PCT_home'],
        row['FT_PCT_home'],
        row['FG3_PCT_home'],
        int(row['AST_home']),
        int(row['REB_home'])
    ))

    session.execute(home_bound_stmt)
    session.execute(away_bound_stmt)

session.execute(batch)