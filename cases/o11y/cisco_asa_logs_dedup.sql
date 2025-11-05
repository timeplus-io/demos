-- Fine-grained deduplication with extracted patterns
-- Extract IPs first, then dedup on device, message_id, and IPs
CREATE STREAM cisco.deduped_asa_logs_detailed
(
  `ingestion_time` datetime64(3),
  `log_timestamp` string,
  `device_name` string,
  `severity` nullable(int8),
  `message_id` string,
  `asa_message` string,
  `src_ip` nullable(string),
  `dst_ip` nullable(string),
  `raw_message` string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW cisco.mv_dedup_detailed
INTO cisco.deduped_asa_logs_detailed AS
WITH extracted AS (
  SELECT
    ingestion_time,
    log_timestamp,
    device_name,
    severity,
    message_id,
    asa_message,
    raw_message,
    -- Extract first IP (source)
    extract(asa_message, 'from ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS src_ip,
    -- Extract second IP (destination) 
    extract(asa_message, 'to ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS dst_ip
  FROM cisco.parsed_asa_logs
)
SELECT
  ingestion_time,
  log_timestamp,
  device_name,
  severity,
  message_id,
  asa_message,
  src_ip,
  dst_ip,
  raw_message
FROM dedup(extracted, device_name, message_id, src_ip, dst_ip, 300s, 500000);


-- Dedup with aggregation (count duplicates before deduping)
-- First aggregate in tumble windows, then deduplicate
CREATE STREAM cisco.deduped_with_counts
(
  `window_start` datetime64(3),
  `device_name` string,
  `severity` nullable(int8),
  `message_id` string,
  `event_count` uint64,
  `sample_message` string,
  `unique_sources` uint64,
  `unique_destinations` uint64
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW cisco.mv_dedup_aggregated
INTO cisco.deduped_with_counts AS
WITH aggregated AS (
  SELECT
    window_start,
    device_name,
    any(severity) AS severity,
    message_id,
    count() AS event_count,
    any(asa_message) AS sample_message,
    uniq(extract(asa_message, 'from ([0-9.]+)')) AS unique_sources,
    uniq(extract(asa_message, 'to ([0-9.]+)')) AS unique_destinations
  FROM tumble(cisco.parsed_asa_logs, 1m)
  GROUP BY window_start, device_name, message_id
)
SELECT
  window_start,
  device_name,
  severity,
  message_id,
  event_count,
  sample_message,
  unique_sources,
  unique_destinations
FROM dedup(aggregated, device_name, message_id, 300s);



-- Simple Fuzzy Dedup - Normalize by removing variable fields
CREATE STREAM cisco.deduped_fuzzy_simple
(
  `ingestion_time` datetime64(3),
  `device_name` string,
  `severity` nullable(int8),
  `message_id` string,
  `asa_message` string,
  `src_ip` nullable(string),
  `dst_ip` nullable(string),
  `msg_hash` uint64,
  `raw_message` string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW cisco.mv_dedup_fuzzy_simple
INTO cisco.deduped_fuzzy_simple AS
WITH normalized AS (
  SELECT
    ingestion_time,
    device_name,
    severity,
    message_id,
    asa_message,
    raw_message,
    -- Extract IPs
    extract(asa_message, 'from ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS src_ip,
    extract(asa_message, 'to ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS dst_ip,
    -- Create fuzzy hash by:
    -- 1. Removing numbers (connection IDs, byte counts, durations)
    -- 2. Keeping message structure and IPs
    murmur_hash3_64(
      concat(
        device_name, '|',
        message_id, '|',
        coalesce(src_ip, ''), '|',
        coalesce(dst_ip, ''),
        -- First 100 chars of message with numbers removed for pattern
        substring(replace_regexp_all(asa_message, '\\d+', 'N'), 1, 100)
      )
    ) AS msg_hash
  FROM cisco.parsed_asa_logs
)
SELECT
  ingestion_time,
  device_name,
  severity,
  message_id,
  asa_message,
  src_ip,
  dst_ip,
  msg_hash,
  raw_message
FROM dedup(normalized, device_name, msg_hash, 300s, 500000);


-- Adjustable Similarity Levels - Coarse, Medium, Fine
CREATE STREAM cisco.deduped_fuzzy_adjustable
(
  `ingestion_time` datetime64(3),
  `device_name` string,
  `message_id` string,
  `similarity_level` string,
  `asa_message` string,
  `coarse_hash` uint64,
  `medium_hash` uint64,
  `fine_hash` uint64,
  `raw_message` string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW cisco.mv_dedup_fuzzy_adjustable
INTO cisco.deduped_fuzzy_adjustable AS
WITH multi_hash AS (
  SELECT
    ingestion_time,
    device_name,
    message_id,
    asa_message,
    raw_message,
    
    -- COARSE: Only device + message_id (catches all similar events)
    murmur_hash3_64(
      concat(device_name, '|', message_id)
    ) AS coarse_hash,
    
    -- MEDIUM: device + message_id + IPs (catches same traffic flows)
    murmur_hash3_64(
      concat(
        device_name, '|',
        message_id, '|',
        coalesce(extract(asa_message, 'from [^:]*:([0-9.]+)'), ''), '→',
        coalesce(extract(asa_message, 'to [^:]*:([0-9.]+)'), '')
      )
    ) AS medium_hash,
    
    -- FINE: device + message_id + IPs + ports + protocol (catches exact flows)
    murmur_hash3_64(
      concat(
        device_name, '|',
        message_id, '|',
        coalesce(extract(asa_message, 'from [^:]*:([0-9.]+)'), ''), ':',
        coalesce(extract(asa_message, 'from [^/]*/([0-9]+)'), ''), '→',
        coalesce(extract(asa_message, 'to [^:]*:([0-9.]+)'), ''), ':',
        coalesce(extract(asa_message, 'to [^/]*/([0-9]+)'), ''), '|',
        coalesce(extract(asa_message, '(TCP|UDP|ICMP)'), '')
      )
    ) AS fine_hash,
    
    -- Choose similarity level based on message type
    multi_if(
      -- Use COARSE for keepalive/hello messages (very repetitive)
      message_id IN ('718012', '718015', '718019', '718021', '718023'), 'coarse',
      -- Use MEDIUM for connection tracking (care about IPs, not ports)
      message_id LIKE '302%', 'medium',
      -- Use FINE for security events (need all details)
      message_id IN ('106023', '106001', '108003', '733102'), 'fine',
      -- Default MEDIUM
      'medium'
    ) AS similarity_level
  FROM cisco.parsed_asa_logs
),
selected_hash AS (
  SELECT
    *,
    multi_if(
      similarity_level = 'coarse', coarse_hash,
      similarity_level = 'fine', fine_hash,
      medium_hash
    ) AS dedup_hash
  FROM multi_hash
)
SELECT
  ingestion_time,
  device_name,
  message_id,
  similarity_level,
  asa_message,
  coarse_hash,
  medium_hash,
  fine_hash,
  raw_message
FROM dedup(selected_hash, device_name, message_id, dedup_hash, 300s, 500000);

