import configparser
from analysis import Analysis
from connection import CassandraConnection
from ingest import Ingest

# Configuration
config = configparser.ConfigParser()
config.read("config.ini")  # Configuration file

HOST = config["CASSANDRA"]["HOST"]
PORT = config["CASSANDRA"]["PORT"]
KEYSPACE = config["CASSANDRA"]["KEYSPACE"]
SCHEMA_SCRIPT = config["CQL"]["SCHEMA"]

if __name__ == "__main__":
    conn = CassandraConnection(HOST, PORT, KEYSPACE)
    session = conn.connect()
    ingest = Ingest(session, config)
    analysis = Analysis(session)

    conn.execute_cql(SCHEMA_SCRIPT)
    conn.set_keyspace(KEYSPACE)

    # ingest.ingest_team_map()
    # ingest.ingest_seasonal_performance()
    # ingest.ingest_game_outcome_performance()
    # ingest.ingest_players_stats()
    
    analysis.point_per_year()
    # analysis.seasonal_analysis(2022, type='avg_points', color='blue', ylabel='Average Points')
    # analysis.outcome_correlation(2022)
    # analysis.player_analysis('Deni Avdija')
    conn.close()