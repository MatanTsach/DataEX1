import pandas as pd
from cassandra.cluster import Cluster
import logging
import configparser


# Configuration
config = configparser.ConfigParser()
config.read("config.ini")  # Configuration file

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest():
    # Connect to Cassandra
    cluster, session = connect_cassandra()
    print("Connected to Cassandra")
    # Execute CQL scripts
    execute_cql_scripts(session, config["CQL"]["SCHEMA"])
    # Ingest data
    games_df = pd.read_csv(config["DATA"]["GAMES_CSV"])
    teams_df = pd.read_csv(config["DATA"]["TEAMS_CSV"])
    # ingest_game_stats(session, games_df)
    # ingest_seasonal_performance(session, games_df)
    ingest_team_map(session, teams_df)
    # games_df = pd.read_csv(config['DATA']['GAMES_CSV'])
    player_details_df = pd.read_csv(config["DATA"]["GAMES_DETAILS_CSV"])

    # ingest_game_stats(session, games_df)
    # ingest_seasonal_performance(session, games_df)
    ingest_player_stats(session, player_details_df)

    return cluster, session


def connect_cassandra():
    cluster = Cluster(
        [config["CASSANDRA"]["HOST"]], port=int(config["CASSANDRA"]["PORT"])
    )
    session = cluster.connect()

    # Create keyspace if it doesn't exist
    create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {config['CASSANDRA']['KEYSPACE']}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        AND durable_writes = true;
    """
    print(f"Executing keyspace creation query:\n{create_keyspace_query}")
    session.execute(create_keyspace_query)

    session.set_keyspace(config["CASSANDRA"]["KEYSPACE"])
    return cluster, session


def execute_cql_scripts(session, cql_script):
    """Execute CQL commands from a .cql file."""
    with open(cql_script, "r") as file:
        cql_commands = file.read()
        for command in cql_commands.split(";"):
            command = command.strip()  # Remove leading/trailing whitespace
            if command:
                logger.info(f"Executing CQL command:\n{command}")
                session.execute(command)


def ingest_seasonal_performance(session, games_df):
    logging.info("Starting to ingest seasonal performance")
    home_averages = (
        games_df.groupby(["HOME_TEAM_ID", "SEASON"])
        .agg(
            {
                "PTS_home": "mean",
                "REB_home": "mean",
                "AST_home": "mean",
                "PTS_away": "mean",
            }
        )
        .reset_index()
        .rename(
            columns={
                "HOME_TEAM_ID": "team_id",
                "PTS_home": "avg_points",
                "REB_home": "avg_rebounds",
                "AST_home": "avg_assists",
                "PTS_away": "avg_opponent_points",
            }
        )
    )

    # Calculate averages for away games
    away_averages = (
        games_df.groupby(["VISITOR_TEAM_ID", "SEASON"])
        .agg(
            {
                "PTS_away": "mean",
                "REB_away": "mean",
                "AST_away": "mean",
                "PTS_home": "mean",
            }
        )
        .reset_index()
        .rename(
            columns={
                "VISITOR_TEAM_ID": "team_id",
                "PTS_away": "avg_points",
                "REB_away": "avg_rebounds",
                "AST_away": "avg_assists",
                "PTS_home": "avg_opponent_points",
            }
        )
    )

    seasonal_averages = pd.concat([home_averages, away_averages])
    seasonal_averages = (
        seasonal_averages.groupby(["team_id", "SEASON"]).mean().reset_index()
    )

    prepared = session.prepare(
        """
        INSERT INTO seasonal_performance (
            season,
            team_id,
            avg_points,
            avg_assists,
            avg_rebounds,
            avg_opponent_points
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    )

    for _, row in seasonal_averages.iterrows():
        session.execute(
            prepared,
            (
                int(row["SEASON"]),
                int(row["team_id"]),
                row["avg_points"],
                row["avg_assists"],
                row["avg_rebounds"],
                row["avg_opponent_points"],
            ),
        )

    logging.info("Finished ingesting seasonal performance")


def ingest_player_stats(session, player_details_df):
    logging.info("Starting to ingest player stats")

    # Select the first 10,000 rows
    selected_rows = player_details_df.head(10000)

    for _, row in selected_rows.iterrows():
        player = str(row["PLAYER_NAME"])

        # Check for NaN values and handle them
        game_id = int(row['GAME_ID']) if not pd.isna(row['GAME_ID']) else None
        reb = int(row['REB']) if not pd.isna(row['REB']) else None
        pts = int(row['PTS']) if not pd.isna(row['PTS']) else None
        ast = int(row['AST']) if not pd.isna(row['AST']) else None

        # Skip the row if any of the essential values are NaN
        if pd.isna(reb) or pd.isna(pts) or pd.isna(ast) or pd.isna(player):
            continue

        session.execute(
            """
            INSERT INTO player_stats (
                player_name,
                game_id,
                reb,
                pts,
                ast
            ) VALUES (%s, %s, %s, %s, %s)
        """, (player, game_id, reb, pts, ast))

    logging.info("Finished ingesting player stats")


def ingest_team_map(session, teams_df):
    logging.info("Starting to ingest team map")
    prepared = session.prepare(
        """
        INSERT INTO teams_map (
            team_id,
            team_name
        ) VALUES (?, ?)
    """
    )

    for _, row in teams_df.iterrows():
        session.execute(
            prepared, (int(row["TEAM_ID"]), f"{row['CITY']} {row['NICKNAME']}")
        )

    logging.info("Finished ingesting team map")


def ingest_game_outcome_performance(session, games_df):
    logging.info("Starting to ingest game outcome performance")
    games_df["home_outcome"] = games_df["HOME_TEAM_WINS"].apply(
        lambda x: "win" if x == 1 else "loss"
    )
    games_df["away_outcome"] = games_df["HOME_TEAM_WINS"].apply(
        lambda x: "loss" if x == 1 else "win"
    )

    prepared = session.prepare(
        """
        INSERT INTO game_outcome_performance (
            season,
            outcome,
            team_id,
            points,
            assists,
            rebounds,
            fg_pct,
            ft_pct,
            fg3_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    for _, row in games_df.iterrows():
        if pd.isna(row["PTS_home"]):
            continue
        # Insert row for the home team
        session.execute(
            prepared,
            (
                int(row["SEASON"]),
                row["home_outcome"],
                row["HOME_TEAM_ID"],
                int(row["PTS_home"]),
                int(row["AST_home"]),
                int(row["REB_home"]),
                row["FG_PCT_home"],
                row["FT_PCT_home"],
                row["FG3_PCT_home"],
            ),
        )

        # Insert row for the away team
        session.execute(
            prepared,
            (
                int(row["SEASON"]),
                row["away_outcome"],
                row["VISITOR_TEAM_ID"],
                int(row["PTS_away"]),
                int(row["AST_away"]),
                int(row["REB_away"]),
                row["FG_PCT_away"],
                row["FT_PCT_away"],
                row["FG3_PCT_away"],
            ),
        )
    logging.info("Finished ingesting game outcome performance")
