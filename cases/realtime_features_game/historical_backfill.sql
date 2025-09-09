-- Historical transactions backfill
CREATE EXTERNAL TABLE game.ex_s3_player_actions
 (
   `user_id` string,
  `session_id` string,
  `timestamp` string,
  `event_type` enum8('match_start' = 1, 'item_pickup' = 2, 'player_elimination' = 3, 'match_end' = 4),
  `game_mode` enum8('battle_royale' = 1, 'team_deathmatch' = 2, 'capture_the_flag' = 3),
  `match_id` string,
  `event_data` string,
  `device_info` string,
  `_tp_time` datetime64(3, 'UTC')
) SETTINGS 
    type = 's3',
    endpoint = 'https://storage.googleapis.com/timeplus-demo',
    access_key_id = 'GOOG1EXR4JXMTFDFYRSH6DKX2CBDEQLNBKOSDKM474P2RIEJIVVGOUQ5VRKEZ',
    secret_access_key = 'AFNU7Ecb/xG/gnZ1ha6qxclElS7L/9vfK+D/YOsv',
    data_format = 'JSONEachRow', write_to = 'game/actions.jsonl',
    s3_min_upload_file_size = 1024,
    s3_max_upload_idle_seconds = 60

-- backup existing data
CREATE MATERIALIZED VIEW game.mv_backup_player_actions 
INTO game.ex_s3_player_actions
AS
SELECT * FROM game.player_actions;

-- backfill historical data into stream
INSERT INTO game.player_actions
SELECT * FROM game.ex_s3_player_actions
