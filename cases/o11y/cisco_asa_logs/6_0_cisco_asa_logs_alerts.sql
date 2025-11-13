-- Critical Security Events (Immediate)
CREATE VIEW cisco_observability.v_alert_critical_events
AS
SELECT
  now64(3) AS alert_time,
  concat('Critical Event: Message ID ', message_id, ' on device ', device_name) AS title,
  concat(
    'Device ', device_name, ' reported a critical event (Severity ', to_string(severity), '). ',
    'Message: ', asa_message
  ) AS description,
  'critical' AS alert_serverity,
  [asa_message] AS raw_events
FROM cisco_observability.flatten_extracted_asa_logs
WHERE severity < 2;

-- Alert 2: Brute Force Detection (5+ failed auth in 5min)
CREATE VIEW cisco_observability.v_alert_brute_force
AS
SELECT
  window_start,
  max(ingestion_time) AS alert_time,
  concat('BRUTE-', to_string(uuid())) AS alert_id,
  'Brute Force Attack' AS alert_type,
  'HIGH' AS severity,
  any(device_name) AS device_name,
  concat('Potential brute force attack on user ', username) AS title,
  concat(
    'Detected ', to_string(count()), ' authentication failures for user "',
    username, '" on AAA server ', any(to_string(aaa_server)), ' in 60 seconds. ',
    'Reasons: ', any(auth_reason)
  ) AS description,
  any(to_string(aaa_server)) AS src_ip,  -- Use AAA server as "source"
  username,  -- Use username as "target"
  count() AS event_count,
  group_array(asa_message) AS raw_events,
  'Block or investigate user account - possible credential stuffing or brute force attack' AS recommended_action
FROM tumble(cisco_observability.flatten_extracted_asa_logs, 5m)
WHERE message_id = '113015'  -- AAA Authentication Rejected
  AND username != ''  -- Ensure username is populated
GROUP BY
  window_start,
  username  -- Group by username being attacked
HAVING count() >= 5
EMIT PERIODIC 60s;

--  DDoS Attack Indicators
CREATE VIEW cisco_observability.v_alert_ddos
AS
SELECT
  window_start,
  max(ingestion_time) AS alert_time,
  'DoS Attack' AS alert_type,
  'critical' AS severity,
  concat('Potential DoS attack targeting ', to_string(dst_ip)) AS title,
  group_array(src_ip) AS src_ips,
  dst_ip,
  count() AS event_count,
  'Enable DoS protection and rate limiting on affected interface' AS recommended_action
FROM hop(cisco_observability.flatten_extracted_asa_logs, 15s,30s)
WHERE dst_ip IS NOT NULL  -- Only events with destination IP
AND _tp_time > now() -10m
GROUP BY
  window_start,
  dst_ip
HAVING event_count > 100 and length(src_ips) > 10;  -- 10 unique source IPs send over 100 events in 30s


-- Alert UDF

CREATE OR REPLACE FUNCTION send_alert_with_webhook(title string, content string, severity string) 
RETURNS string 
LANGUAGE PYTHON AS $$
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

-- Critical Alert

CREATE ALERT cisco_observability.critical_event_alert
BATCH 1 EVENTS WITH TIMEOUT 5s
LIMIT 1 ALERTS PER 15s
CALL send_alert_with_webhook
AS 
SELECT
  title,
  description as content,
  alert_serverity as severity
FROM
  cisco_observability.v_alert_critical_events;

DROP ALERT IF EXISTS cisco_observability.critical_event_alert;

-- check the alert @ http://34.168.13.2
