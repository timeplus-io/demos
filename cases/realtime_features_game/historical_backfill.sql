-- Historical transactions backfill
CREATE EXTERNAL STREAM game.ex_3s_player_actions
 (
   `user_id` string,
  `session_id` string,
  `timestamp` string,
  `event_type` enum8('match_start' = 1, 'item_pickup' = 2, 'player_elimination' = 3, 'match_end' = 4),
  `game_mode` enum8('battle_royale' = 1, 'team_deathmatch' = 2, 'capture_the_flag' = 3),
  `match_id` string,
  `event_data` string,
  `device_info` string

) SETTINGS 
    type='s3', 
 path='s3://gaming-data/historical_transactions/year=2024/**/*.parquet',
    data_format='Parquet';

-- backfill historical data into stream
INSERT INTO game.player_actions
SELECT * FROM game.ex_3s_player_actions
