CREATE DATABASE IF NOT EXISTS stream_processing;

-- external streams

CREATE STREAM stream_processing.eventstream
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'owlshop-frontend-events';

CREATE STREAM stream_processing.ext_kafka_mv_hourly_count
(
  `start_of_hour` datetime64(3, 'UTC'),
  `repo` string,
  `event_num` uint64
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'top_repo_each_hour'
COMMENT 'external kafka stream for sink mv_hourly_count';

CREATE STREAM stream_processing.github_events
(
  `actor` string,
  `created_at` string,
  `id` string,
  `payload` string,
  `repo` string,
  `type` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'github_events', data_format = 'JSONEachRow', one_message_per_row = true
COMMENT 'an external stream to read GitHub events in JSON format ';

-- external tables
CREATE EXTERNAL TABLE stream_processing.ch_events
SETTINGS type = 'clickhouse', address = '34.169.40.46:9000', user = 'demo', password = 'demo123', database = 'demo', table = 'events';

CREATE EXTERNAL TABLE stream_processing.dim_code_to_status
SETTINGS type = 'clickhouse', address = '34.169.40.46:9000', user = 'demo', password = 'demo123', database = 'demo', table = 'http_status_codes';

CREATE EXTERNAL TABLE stream_processing.http_code_count_5s
SETTINGS type = 'clickhouse', address = '34.169.40.46:9000', user = 'demo', password = 'demo123', database = 'demo', table = 'http_code_count_5s';

-- views
CREATE VIEW stream_processing.parsedstream
AS
SELECT
  _tp_time, cast(raw:requestDuration, 'float') AS requestDuration
FROM
  stream_processing.eventstream;

CREATE VIEW stream_processing.v_AverageAggregation
AS
SELECT
  window_end, avg(requestDuration) AS latestDuration
FROM
  tumble(stream_processing.parsedstream, 1s)
GROUP BY
  window_end;

CREATE VIEW stream_processing.v_BandCalculation
AS
WITH stdevCalc AS
  (
    SELECT
      window_end, group_array(requestDuration) AS X, array_avg(X) AS mean, sqrt(array_avg(array_map(x -> ((x - mean) * (x - mean)), X))) AS std, latest(_tp_time) AS latestTime
    FROM
      tumble(stream_processing.parsedstream, 10s)
    GROUP BY
      window_end
  )
SELECT
  window_end, latestTime, mean + (2 * std) AS hiBand, mean - (2 * std) AS loBand
FROM
  stdevCalc
ORDER BY
  window_end ASC;

CREATE VIEW stream_processing.v_CreateVisualization
AS
SELECT
  *
FROM
  (
    SELECT
      window_end, latestDuration AS value, 'livedata' AS key
    FROM
      stream_processing.v_AverageAggregation
    UNION ALL
    SELECT
      window_end, hiBand AS value, 'hiBand' AS key
    FROM
      stream_processing.v_BandCalculation
    UNION ALL
    SELECT
      window_end, loBand AS value, 'loBand' AS key
    FROM
      stream_processing.v_BandCalculation
  );

-- MV

CREATE MATERIALIZED VIEW stream_processing.mv_5s_tumble_then_join INTO stream_processing.http_code_count_5s
AS
WITH statusCode AS
  (
    SELECT
      _tp_time, cast(raw:response.statusCode, 'uint8') AS code
    FROM
      stream_processing.eventstream
  ), countByStatus AS
  (
    SELECT
      window_start, code, count() AS views
    FROM
      tumble(statusCode, 5s)
    GROUP BY
      window_start, code
  )
SELECT
  window_start AS ts, code, status, views
FROM
  countByStatus
INNER JOIN stream_processing.dim_code_to_status USING (code);

CREATE MATERIALIZED VIEW stream_processing.mv_mask_ip INTO stream_processing.ch_events
AS
SELECT
  now64() AS _tp_time, raw:requestedUrl AS url, raw:method AS method, lower(hex(md5(raw:ipAddress))) AS ip
FROM
  stream_processing.eventstream;

CREATE MATERIALIZED VIEW stream_processing.mv_mv_hourly_count INTO stream_processing.ext_kafka_mv_hourly_count
AS
SELECT
  window_start AS start_of_hour, repo, count() AS event_num
FROM
  tumble(stream_processing.github_events, 1h)
GROUP BY
  window_start, repo
ORDER BY
  event_num DESC
LIMIT 1 BY
  window_start
SETTINGS
  seek_to = '-6h';