
-- total first, and lastgame played, 
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

-- total elimination count, distinct game played
SELECT 
    user_id, 
    count_if(event_type = 'player_elimination') AS total_elimination_count,
    count_distinct(game_mode) AS distinct_game_played
FROM game.player_actions
WHERE _tp_time > earliest_ts()
GROUP BY user_id
settings seek_to = 'earliest';

-- Performance by Game Mode
-- “Does the user’s FPS drop in battle_royale vs team_deathmatch?”
SELECT
    pa.user_id,
    pa.game_mode,
    avg(pm.device_stats:fps_avg::float) AS avg_fps_by_mode,
    avg(pm.device_stats:network_latency_ms::float) AS avg_latency_by_mode
FROM game.player_actions pa
JOIN game.performance_metrics pm
  ON pa.user_id = pm.user_id
 AND pa.session_id = pm.session_id
GROUP BY pa.user_id, pa.game_mode
settings seek_to = 'earliest';

-- Session Length vs Performance Degradation
SELECT
    pa.user_id,
    avg(pm.device_stats:battery_level::float) AS avg_battery_level,
    count_distinct(pa.match_id) AS session_games
FROM game.player_actions pa
JOIN game.performance_metrics pm
  ON pa.user_id = pm.user_id
 AND pa.session_id = pm.session_id
GROUP BY pa.user_id
settings seek_to = 'earliest';

-- “Does network latency spike when the user eliminates or gets eliminated?”
SELECT
  pa.user_id, 
  avg_if(pm.device_stats:network_latency_ms::float, pa.event_type = 'player_elimination') AS avg_latency_during_elim, 
  avg_if(pm.device_stats:network_latency_ms::float, pa.event_type = 'item_pickup') AS avg_latency_during_pickup,
  avg(pm.device_stats:network_latency_ms::float) AS avg_all
FROM
  game.player_actions AS pa
INNER JOIN game.performance_metrics AS pm ON (pa.user_id = pm.user_id) AND (pa.session_id = pm.session_id)
GROUP BY
  pa.user_id
SETTINGS
  seek_to = 'earliest';




