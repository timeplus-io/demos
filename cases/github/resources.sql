
CREATE DATABASE IF NOT EXISTS github;

CREATE STREAM github.github_events
(
  `actor` string,
  `created_at` string,
  `id` string,
  `payload` string,
  `repo` string,
  `type` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', 
    brokers = '10.138.0.23:9092', 
    topic = 'github_events', 
    data_format = 'JSONEachRow', 
    one_message_per_row = true
COMMENT ' ';

CREATE MATERIALIZED VIEW github.mv_github_events
(
  `_tp_time` datetime64(3, 'UTC'),
  `actor` string,
  `created_at` datetime,
  `id` string,
  `payload` string,
  `repo` string,
  `type` string,
  `_tp_sn` int64
) AS
SELECT
  _tp_time, actor, cast(created_at, 'datetime') AS created_at, id, payload, repo, type
FROM
  github.github_events
SETTINGS
  seek_to = '231000000,70000000,70000000'
STORAGE_SETTINGS index_granularity = 8192