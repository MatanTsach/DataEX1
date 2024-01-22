CREATE KEYSPACE IF NOT EXISTS nba_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS game_stats_outcome (
    game_id bigint,
    team_id bigint,
    opponent_team_id bigint,
    season int,
    game_type text,
    team_points int,
    opponent_points int,
    team_ast int,
    team_reb int,
    team_fg_pct double,
    team_fg3_pct double,
    game_outcome text,  -- 'win' or 'loss'
    PRIMARY KEY ((team_id), season, game_type, game_outcome)
) WITH CLUSTERING ORDER BY (season DESC, game_type ASC, game_outcome ASC);

CREATE TABLE IF NOT EXISTS seasonal_performance (
    team_id bigint,
    season int,
    avg_points double,
    avg_rebounds double,
    avg_assists double,
    PRIMARY KEY ((team_id), season)
);

CREATE TABLE IF NOT EXISTS home_away_performance (
    team_id bigint,
    season int,
    game_type text,
    avg_points double,
    avg_rebounds double,
    avg_assists double,
    PRIMARY KEY ((team_id), season, game_type)
);
