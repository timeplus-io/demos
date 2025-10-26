
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
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';



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
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

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


----------------------------------------------------------------------------------------
-- It uses hopping windows of 1 hour with a hop size of 5 minutes to aggregate
CREATE MATERIALIZED VIEW game.mv_player_features_1h AS
SELECT 
    user_id,
    window_start as ts,
    window_end as te,
    count(*) as total_events_1h,
    count_distinct(timestamp) as active_days_1h,
    count_distinct(session_id) as total_sessions_1h,
    sum(event_data:kills::float) as total_kills_1h,
    count() FILTER(WHERE event_data:kills::float <= 3) as top3_finishes_1h,
    count_distinct(device_info:platform) as platforms_used_1h
FROM hop(game.player_actions, 5m, 1h)
WHERE _tp_time > earliest_ts()
GROUP BY user_id, window_start, window_end;

-- 1-day monetization and engagement features
-- It uses hopping windows of 1 day with a hop size of 1 hour to aggregate both transaction and social interaction data.
CREATE MATERIALIZED VIEW game.mv_engagement_features_1d AS
SELECT 
    t.user_id,
    t.window_start as ts,
    t.window_end as te,
    -- Transaction features
    coalesce(sum(t.amount_usd), 0) as total_revenue_1d,
    coalesce(count(t.transaction_id), 0) as transaction_count_1d,
    coalesce(count_distinct(t.item_category), 0) as categories_purchased_1d,
    -- Social features from join
    coalesce(count(s.interaction_type), 0) as social_interactions_1d,
    coalesce(count_distinct(s.target_user_id), 0) as unique_friends_1d
FROM hop(game.transactions, 1h, 1d) t
LEFT JOIN hop(game.social_events, 1h, 1d) s 
    ON t.user_id = s.user_id 
    AND t.window_start = s.window_start
    AND date_diff_within(2m, t.window_start, s.window_start)  -- ensure events are within the same time window
GROUP BY t.user_id, t.window_start, t.window_end
settings seek_to = 'earliest'