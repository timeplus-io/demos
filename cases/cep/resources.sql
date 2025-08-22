CREATE DATABASE IF NOT EXISTS cep;

-- simulated data source with random streams

CREATE RANDOM STREAM cep.cep_test_stream
(
  `time` datetime64(3) DEFAULT now64(),
  `event` string DEFAULT ['A', 'B', 'C'][(rand() % 3) + 1],
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  `_tp_sn` int64 CODEC(Delta(8), ZSTD(1)),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 32,
  INDEX _tp_sn_index _tp_sn TYPE minmax GRANULARITY 32
)
SETTINGS eps = 5;

CREATE RANDOM STREAM cep.login_events
(
  `eventType` string DEFAULT ['login', 'logout'][(rand() % 2) + 1],
  `userId` string DEFAULT ['user123', 'user456'][(rand() % 2) + 1],
  `location` string DEFAULT ['New York', 'Berlin', 'Vancouver'][(rand() % 3) + 1],
  `timestamp` datetime64(3) DEFAULT now64(),
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  `_tp_sn` int64 CODEC(Delta(8), ZSTD(1)),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 32,
  INDEX _tp_sn_index _tp_sn TYPE minmax GRANULARITY 32
)
SETTINGS eps = 5;

CREATE RANDOM STREAM cep.purchase_events
(
  `eventType` string DEFAULT ['purchase', 'checkout'][(rand() % 2) + 1],
  `userId` string DEFAULT ['user123', 'user456'][(rand() % 2) + 1],
  `amount` int32 DEFAULT rand() % 1001,
  `location` string DEFAULT ['New York', 'London', 'Tokyo'][(rand() % 3) + 1],
  `timestamp` datetime64(3) DEFAULT now64(),
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  `_tp_sn` int64 CODEC(Delta(8), ZSTD(1)),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 32,
  INDEX _tp_sn_index _tp_sn TYPE minmax GRANULARITY 32
)
SETTINGS eps = 5;

-- views to unify the streams

CREATE VIEW cep.unified_user_events
(
  `eventType` string,
  `userId` string,
  `amount` float64,
  `location` string,
  `timestamp` datetime64(3),
  `source_stream` string
) AS
SELECT
  eventType, userId, 0. AS amount, location, timestamp, eventType AS source_stream
FROM
  cep.login_events
UNION ALL
SELECT
  eventType, userId, amount, location, timestamp, eventType AS source_stream
FROM
  cep.purchase_events;

-- MV sql based complex event processing
CREATE MATERIALIZED VIEW cep.sql_based_cep_fraud_detection
(
  `userId` string,
  `total_events` uint64,
  `login_count` uint64,
  `purchase_count` uint64,
  `total_purchase_amount` float64,
  `all_locations` array(string),
  `event_sequence` array(string),
  `time_sequence` array(datetime64(3)),
  `t_start` datetime64(3),
  `t_end` datetime64(3),
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS
SELECT
  userId, count(*) AS total_events, count_if(eventType = 'login') AS login_count, count_if(eventType = 'purchase') AS purchase_count, sum(amount) AS total_purchase_amount, group_array(location) AS all_locations, group_array(eventType) AS event_sequence, group_array(timestamp) AS time_sequence, window_start AS t_start, window_end AS t_end
FROM
  hop(cep.unified_user_events, timestamp, 5s, 10m)
GROUP BY
  userId, window_start, window_end
HAVING
  (length(array_distinct(all_locations)) >= 2) AND (total_purchase_amount > 1000) AND (purchase_count >= 1);
