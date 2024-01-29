import pandas as pd
from cassandra.cluster import Cluster
import logging
import configparser


class Ingest:
    def __init__(self, session, config):
        self.session = session
        self.games_df = pd.read_csv(config["DATA"]["GAMES_CSV"])
        self.game_details_df = pd.read_csv(config["DATA"]["GAMES_DETAILS_CSV"])
        self.teams_df = pd.read_csv(config["DATA"]["TEAMS_CSV"])

    def ingest_seasonal_performance(self):
        logging.info("Starting to ingest seasonal performance")
        home_averages = (
            self.games_df.groupby(["HOME_TEAM_ID", "SEASON"])
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
            self.games_df.groupby(["VISITOR_TEAM_ID", "SEASON"])
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

        prepared = self.session.prepare(
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
            self.session.execute(
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


    def ingest_players_stats(self):
        logging.info("Starting to ingest player stats")

        ingest_df = self.game_details_df.merge(self.games_df[['GAME_ID', 'SEASON']], on='GAME_ID')
        ingest_df = (
            ingest_df.groupby(["PLAYER_NAME", "SEASON"])
            .agg({"PTS": "mean", "REB": "mean", "AST": "mean", "GAME_ID": "count"})
            .reset_index()
        )
        ingest_df = ingest_df.rename(columns={"GAME_ID": "GAMES_PLAYED"})
        print(ingest_df.head())

        for _, row in ingest_df.iterrows():
            # Skip the row if any of the essential values are NaN
            if pd.isna(row["PTS"]) or pd.isna(row["REB"]) or pd.isna(row["AST"]):
                continue

            prepared = self.session.prepare("""
                    INSERT INTO player_stats (
                        player_name,
                        season,
                        games_played,
                        pts,
                        reb,
                        ast
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """
            )

            self.session.execute(
                prepared,
                (
                    row['PLAYER_NAME'],
                    int(row["SEASON"]),
                    int(row["GAMES_PLAYED"]),
                    float(row["PTS"]),
                    float(row["REB"]),
                    float(row["AST"])
                )
            )

        logging.info("Finished ingesting player stats")


    def ingest_team_map(self):
        logging.info("Starting to ingest team map")
        prepared = self.session.prepare(
            """
            INSERT INTO teams_map (
                team_id,
                team_name
            ) VALUES (?, ?)
        """
        )

        for _, row in self.teams_df.iterrows():
            self.session.execute(
                prepared, (int(row["TEAM_ID"]), f"{row['CITY']} {row['NICKNAME']}")
            )

        logging.info("Finished ingesting team map")


    def ingest_game_outcome_performance(self):
        logging.info("Starting to ingest game outcome performance")
        self.games_df["home_outcome"] = self.games_df["HOME_TEAM_WINS"].apply(
            lambda x: "win" if x == 1 else "loss"
        )
        self.games_df["away_outcome"] = self.games_df["HOME_TEAM_WINS"].apply(
            lambda x: "loss" if x == 1 else "win"
        )

        prepared = self.session.prepare(
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

        for _, row in self.games_df.iterrows():
            if pd.isna(row["PTS_home"]):
                continue
            # Insert row for the home team
            self.session.execute(
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
            self.session.execute(
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
