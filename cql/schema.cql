CREATE KEYSPACE IF NOT EXISTS nba_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE nba_data;

CREATE TABLE IF NOT EXISTS teams_map (
    team_id bigint,
    team_name text,
    PRIMARY KEY ((team_id))
);

CREATE TABLE IF NOT EXISTS seasonal_performance (
    season int,
    team_id bigint,
    avg_points double,
    avg_assists double,
    avg_rebounds double,
    avg_opponent_points double,
    PRIMARY KEY ((season), team_id)
);

CREATE TABLE IF NOT EXISTS game_outcome_performance (
    season int,
    outcome text,
    team_id bigint,
    points double,
    assists double,
    rebounds double,
    fg_pct double,
    ft_pct double,
    fg3_pct double,
    PRIMARY KEY ((season, outcome), team_id)
);
CREATE TABLE IF NOT EXISTS player_stats (
    player_name text,
    season int,
    games_played int,
    pts double,
    reb double,
    ast double,
    PRIMARY KEY ((player_name), season)
);
