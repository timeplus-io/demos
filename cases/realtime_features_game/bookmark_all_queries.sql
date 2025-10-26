
-- temporal window features

-- It uses tumbling windows of 5 minutes to aggregate player actions.   
CREATE STREAM game.player_features_5m
(
  `user_id` string,
  `ts` datetime64(3, 'UTC'),
  `te` datetime64(3, 'UTC'),
  `events_5m` uint64,
  `matches_started_5m` uint64,
  `matches_completed_5m` uint64,
  `avg_kills_5m` float64,
  `max_damage_5m` float32,
  `unique_matches_5m` uint64
);


CREATE MATERIALIZED VIEW game.mv_player_features_5m
INTO game.player_features_5m
AS
SELECT 
    user_id,
    window_start as ts,
    window_end as te,
    count(*) as events_5m,
    count() FILTER(WHERE event_type = 'match_start') as matches_started_5m,
    count() FILTER(WHERE event_type = 'match_end') as matches_completed_5m,
    avg(event_data:kills::float) as avg_kills_5m,
    max(event_data:damage_dealt::float) as max_damage_5m,
    count_distinct(match_id) as unique_matches_5m
FROM tumble(game.player_actions, 5m)
WHERE _tp_time > earliest_ts()
GROUP BY user_id, window_start, window_end;

-- It uses tumbling windows of 15 minutes to aggregate transaction data.

CREATE STREAM game.transaction_features_15m
(
  `user_id` string,
  `ts` datetime64(3, 'UTC'),
  `te` datetime64(3, 'UTC'),
  `transaction_count_15m` uint64,
  `total_spent_15m` float64,
  `avg_transaction_15m` float64,
  `max_transaction_15m` float64,
  `unique_categories_15m` uint64,
  `unique_devices_15m` uint64,
  `unique_cities_15m` uint64
);

CREATE MATERIALIZED VIEW game.mv_transaction_features_15m
INTO game.transaction_features_15m
AS
SELECT 
    user_id,
    window_start as ts,
    window_end as te,
    count(*) as transaction_count_15m,
    sum(amount_usd) as total_spent_15m,
    avg(amount_usd) as avg_transaction_15m,
    max(amount_usd) as max_transaction_15m,
    count_distinct(item_category) as unique_categories_15m,
    count_distinct(device_fingerprint) as unique_devices_15m,
    count_distinct(location:city) as unique_cities_15m
FROM tumble(game.transactions, 15m)
WHERE _tp_time > earliest_ts()
GROUP BY user_id, window_start, window_end;

-- accumulative features
-- total first, and lastgame played, 
CREATE STREAM game.user_game_stats_feature
(
  `user_id` string,
  `total_game_played` uint64,
  `first_game_played` string,
  `last_game_played` string
);

CREATE MATERIALIZED VIEW game.mv_user_game_stats_feature
INTO game.user_game_stats_feature
AS
SELECT
  user_id, 
  count_distinct(match_id) AS total_game_played,
  earliest(match_id) AS first_game_played,
  latest(match_id) AS last_game_played
FROM
  game.player_actions
WHERE
  event_type = 'match_start' and _tp_time > earliest_ts()
GROUP BY
  user_id
settings seek_to = 'earliest';

-- Performance by Game Mode
-- “Does the user’s FPS drop in battle_royale vs team_deathmatch?”
CREATE STREAM game.user_technical_performance_feature
(
  `user_id` string,
  `game_mode` enum8('battle_royale' = 1, 'team_deathmatch' = 2, 'capture_the_flag' = 3),
  `avg_fps_by_mode` float64,
  `avg_latency_by_mode` float64
);

CREATE MATERIALIZED VIEW game.mv_user_technical_performance_feature
INTO game.user_technical_performance_feature
AS
SELECT
    pa.user_id,
    pa.game_mode,
    avg(pm.device_stats:fps_avg::float) AS avg_fps_by_mode,
    avg(pm.device_stats:network_latency_ms::float) AS avg_latency_by_mode
FROM game.player_actions pa
JOIN game.performance_metrics pm
  ON pa.user_id = pm.user_id
 AND pa.session_id = pm.session_id
 AND date_diff_within(2m) -- add time difference condition to join
GROUP BY pa.user_id, pa.game_mode
settings seek_to = 'earliest';

----------------------------------------------------------------------------------------

-- record/event based features
-- total spend in last 10 transactions
CREATE STREAM game.total_spend_last_10_transaction
(
  `user_id` string,
  `total_spend` float64
);

CREATE MATERIALIZED VIEW game.mv_total_spend_last_10_transaction
INTO game.total_spend_last_10_transaction
AS
select 
    user_id, 
    array_sum(x->x, group_array_last (amount_usd, 10)) as total_spend
from game.transactions
group by user_id;

----------------------------------------------------------------------------------------

-- ASOF JOIN all features
-- Combine all features into one view for easy access
-- This view can be queried directly for real-time analytics or used as input to machine learning models training and inference.
CREATE VIEW game.mv_features_all AS
SELECT
  tsl10t._tp_time as time, 
  tsl10t.user_id as user_id, 
  tsl10t.total_spend as total_spend, 
  ugsf.total_game_played as total_game_played,
  ugsf.first_game_played as first_game_played,
  ugsf.last_game_played as last_game_played,
  utpf.game_mode as game_mode,
  utpf.avg_fps_by_mode as avg_fps_by_mode,
  utpf.avg_latency_by_mode as avg_latency_by_mode,
  pf5m.matches_started_5m as matches_started_5m,
  pf5m.matches_completed_5m as matches_completed_5m,
  pf5m.avg_kills_5m as avg_kills_5m,
  pf5m.max_damage_5m as max_damage_5m,
  pf5m.unique_matches_5m as unique_matches_5m,
  tf15m.transaction_count_15m as transaction_count_15m,
  tf15m.total_spent_15m as total_spent_15m,
  tf15m.avg_transaction_15m as avg_transaction_15m,
  tf15m.max_transaction_15m as max_transaction_15m,
  tf15m.unique_categories_15m as unique_categories_15m,    
  tf15m.unique_devices_15m as unique_devices_15m,
  tf15m.unique_cities_15m as unique_cities_15m
FROM
  game.total_spend_last_10_transaction as tsl10t
ASOF LEFT JOIN game.user_game_stats_feature ugsf on tsl10t.user_id = ugsf.user_id 
AND (tsl10t._tp_time >= ugsf._tp_time)
ASOF LEFT JOIN game.user_technical_performance_feature utpf on tsl10t.user_id = utpf.user_id 
AND (tsl10t._tp_time >= utpf._tp_time)
ASOF LEFT JOIN game.player_features_5m as pf5m on tsl10t.user_id = pf5m.user_id 
AND (tsl10t._tp_time >= pf5m.te)
ASOF LEFT JOIN game.transaction_features_15m as tf15m on tsl10t.user_id = tf15m.user_id
AND (tsl10t._tp_time >= tf15m.te)
-- settings seek_to = '-15m'

----------------------------------------------------------------------------------------
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
