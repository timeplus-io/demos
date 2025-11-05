
-- Simplified parsed stream with only routing fields
CREATE STREAM cisco.parsed_asa_logs
(
  `ingestion_time` datetime64(3),
  `raw_message` string,
  `log_timestamp` string,
  `device_name` string,
  `severity` nullable(int8),
  `message_id` string,
  `asa_message` string
) 
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

-- Parse only common fields needed for routing
CREATE MATERIALIZED VIEW cisco.mv_parse_asa_logs 
INTO cisco.parsed_asa_logs AS
SELECT
  now64(3) AS ingestion_time,
  message AS raw_message,
  to_string(base_fields['timestamp']) AS log_timestamp,
  to_string(base_fields['device_name']) AS device_name,
  to_int8_or_null(base_fields['severity']) AS severity,
  to_string(base_fields['message_id']) AS message_id,
  to_string(base_fields['asa_message']) AS asa_message
FROM (
  SELECT
    message,
    grok(message, '%{DATA:timestamp} %{NOTSPACE:device_name}: \\%ASA-%{INT:severity}-%{INT:message_id}: %{GREEDYDATA:asa_message}') AS base_fields
  FROM cisco.asa_logs_stream
)
WHERE base_fields['message_id'] IS NOT NULL;