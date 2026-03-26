CREATE DATABASE IF NOT EXISTS cisco_observability_ddos;

CREATE VIEW IF NOT EXISTS cisco_observability_ddos.v_asa_logs AS
SELECT
  _tp_time,device_name, severity, message_id, asa_message, src_ip, dst_ip, bytes
FROM
  cisco_observability.flatten_extracted_asa_logs
WHERE
  bytes IS NOT NULL;


-- Step 1 — Live 5-second traffic
CREATE STREAM cisco_observability_ddos.sig_live_5s_traffic (
    src_ip ipv4,
    live_bytes uint64,
    w_start datetime64(3, 'UTC'),
    w_end   datetime64(3, 'UTC')
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


CREATE MATERIALIZED VIEW cisco_observability_ddos.mv_live_5s_traffic
INTO cisco_observability_ddos.sig_live_5s_traffic
AS
SELECT
    src_ip,
    sum(bytes) AS live_bytes,
    window_start AS w_start,
    window_end   AS w_end
FROM hop(cisco_observability_ddos.v_asa_logs, 1s, 5s)
WHERE src_ip IS NOT NULL
GROUP BY src_ip, window_start, window_end;

-- Step 2 — 5-minute baseline

-- overall baseline
CREATE MUTABLE STREAM cisco_observability_ddos.sig_overall_baseline_mut (
    src_ip ipv4,
    avg_baseline_bytes float64
)
PRIMARY KEY src_ip;

CREATE MATERIALIZED VIEW cisco_observability_ddos.mv_overall_baseline
INTO cisco_observability_ddos.sig_overall_baseline_mut
AS
SELECT
    src_ip,
    avg(sum_bytes) AS avg_baseline_bytes
FROM (
    SELECT
        window_start as w_start,
        src_ip,
        sum(bytes) AS sum_bytes
    FROM tumble(cisco_observability_ddos.v_asa_logs, 5s)
    WHERE src_ip IS NOT NULL
    GROUP BY window_start, src_ip
)
GROUP BY src_ip;

-- hourly baseline
CREATE MUTABLE STREAM cisco_observability_ddos.sig_hourly_baseline_mut (
    src_ip ipv4,
    hour_of_day uint8,
    avg_baseline_bytes float64
) 
PRIMARY KEY (src_ip, hour_of_day);

CREATE MATERIALIZED VIEW cisco_observability_ddos.mv_hourly_baseline
INTO cisco_observability_ddos.sig_hourly_baseline_mut
AS
SELECT
    src_ip,
    hour_of_day,
    avg(sum_bytes) AS avg_baseline_bytes
FROM (
    SELECT
        window_start as w_start,
        src_ip,
        hour(window_start) AS hour_of_day,
        sum(bytes) AS sum_bytes
    FROM tumble(cisco_observability_ddos.v_asa_logs, 5s)
    WHERE src_ip IS NOT NULL
    GROUP BY src_ip, window_start
)
GROUP BY src_ip, hour_of_day;

-- Step 3 — Spike detection

-- using overall baseline
CREATE STREAM cisco_observability_ddos.cxt_ddos_stream (
    src_ip ipv4,
    live_bytes uint64,
    overall_baseline float64,
    hourly_baseline float64,
    overall_spike_ratio float64,
    hourly_spike_ratio float64
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


CREATE MATERIALIZED VIEW cisco_observability_ddos.mv_cxt_ddos
INTO cisco_observability_ddos.cxt_ddos_stream
AS
SELECT
    l.src_ip as src_ip,
    l.live_bytes as live_bytes,
    o.avg_baseline_bytes AS overall_baseline,
    h.avg_baseline_bytes AS hourly_baseline,
    l.live_bytes / o.avg_baseline_bytes AS overall_spike_ratio,
    l.live_bytes / h.avg_baseline_bytes AS hourly_spike_ratio
FROM cisco_observability_ddos.sig_live_5s_traffic l
JOIN cisco_observability_ddos.sig_overall_baseline_mut o
    ON l.src_ip = o.src_ip
JOIN cisco_observability_ddos.sig_hourly_baseline_mut h
    ON l.src_ip = h.src_ip
    AND h.hour_of_day = hour(l._tp_time);

-- Step 4 — Alerting

SELECT
  src_ip, live_bytes, overall_spike_ratio, hourly_spike_ratio
FROM
  cisco_observability_ddos.cxt_ddos_stream
WHERE
  (overall_spike_ratio > 10) OR (hourly_spike_ratio > 10);


CREATE OR REPLACE FUNCTION send_alert_with_webhook(title string, content string, severity string) RETURNS string LANGUAGE PYTHON AS $$
import json
import requests

def send_alert_with_webhook(title, content, severity):
    results = []
    for title, content, severity in zip(title, content, severity):
        requests.post(
          "http://34.168.13.2/alert",
          data=json.dumps({
              "title": title,
              "message": f"alert with log: {content}",
              "severity": severity
          })
        )
        results.append("OK")
    
    return results
$$

-- set the threshold to 10
CREATE ALERT cisco_observability_ddos.ddos_alert
BATCH 10 EVENTS WITH TIMEOUT 5s
LIMIT 1 ALERTS PER 30s
CALL send_alert_with_webhook
AS
SELECT
    'DDoS Attack Detected' AS title,
    concat(
        '🚨 Source IP: ', to_string(src_ip), '\n',
        '📊 Live Traffic (5s): ', format_readable_size(live_bytes), '\n',
        '📈 Overall Spike Ratio: ', to_string(round(overall_spike_ratio, 2)), 'x',
        CASE WHEN overall_spike_ratio > 10 THEN ' ⚠️ EXCEEDED' ELSE '' END, '\n',
        '📈 Hourly Spike Ratio: ', to_string(round(hourly_spike_ratio, 2)), 'x',
        CASE WHEN hourly_spike_ratio > 10 THEN ' ⚠️ EXCEEDED' ELSE '' END, '\n',
        '🕐 Time: ', to_string(now())
    ) AS content,
    'high' AS severity
FROM cisco_observability_ddos.cxt_ddos_stream
WHERE overall_spike_ratio > 10 OR hourly_spike_ratio > 10;

-- check the alert from http://34.168.13.2/



