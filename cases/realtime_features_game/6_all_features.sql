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

