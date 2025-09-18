CREATE DATABASE IF NOT EXISTS ocsf;

CREATE STREAM ocsf.ocsf_events
(
  `raw` string
)
TTL to_datetime(_tp_time) + 1d
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';