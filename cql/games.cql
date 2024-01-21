CREATE KEYSPACE IF NOT EXISTS nba_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE nba_data;

DROP TABLE IF EXISTS games;
CREATE TABLE IF NOT EXISTS games (
    team_id bigint,
    game_type text,
    season bigint,
    rival_team_id bigint,
    game_id bigint,
    team_pts int,
    team_pg_pct double,
    team_ft_pct double,
    team_fg3_pct double,
    team_ast int,
    team_reb int,
    rival_pts int,
    rival_pg_pct double,
    rival_ft_pct double,
    rival_ast_pct double,
    rival_ast int,
    rival_reb int,

    PRIMARY KEY ((team_id), game_type, season)
);