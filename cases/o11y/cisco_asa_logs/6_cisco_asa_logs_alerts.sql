-- Critical Security Events (Immediate)
CREATE VIEW cisco_observability.v_alert_critical_events
AS
SELECT
  now64(3) AS alert_time,
  concat('CRIT-', to_string(uuid())) AS alert_id,
  'Critical Security Event' AS alert_type,
  device_name,
  concat('Critical Event: ', message_category, ' (', message_id, ')') AS title,
  concat(
    'Device ', device_name, ' reported a critical event. ',
    'Message: ', asa_message
  ) AS description,
  coalesce(src_ip, 'N/A') AS src_ip,
  coalesce(dst_ip, 'N/A') AS dst_ip,
  1 AS event_count,
  [asa_message] AS raw_events,
  multi_if(
    message_id = '106022', 'Investigate connection spoofing attempt - possible attack',
    message_id = '108003', 'Block source IP - SMTP malicious pattern detected',
    message_id = '202010', 'Review NAT pool configuration - capacity issue',
    message_id = '702307', 'Expand NAT pool - exhaustion detected',
    'Investigate immediately and review security posture'
  ) AS recommended_action
FROM cisco_observability.enhanced_asa_logs
WHERE is_critical = true
  AND severity <= 2;

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
  concat('Potential brute force attack from ', src_ip) AS title,
  concat(
    'Detected ', to_string(count()), ' authentication failures from ',
    src_ip, ' to ', dst_ip, ' in 60 seconds'
  ) AS description,
  src_ip,
  any(dst_ip) AS dst_ip,
  count() AS event_count,
  group_array(asa_message) AS raw_events,
  'Block source IP and investigate user account security' AS recommended_action
FROM tumble(cisco_observability.enhanced_asa_logs, 5m)
WHERE message_category = 'Authentication'
  AND action = 'deny'
GROUP BY
  window_start,
  src_ip
HAVING count() >= 5
EMIT PERIODIC 60s;

--  DDoS Attack Indicators
CREATE VIEW cisco_observability.v_alert_dos
AS
SELECT
  max(ingestion_time) AS alert_time,
  concat('DOS-', to_string(city_hash64(dst_ip))) AS alert_id,
  'DoS Attack' AS alert_type,
  'CRITICAL' AS severity,
  any(device_name) AS device_name,
  concat('Potential DoS attack targeting ', dst_ip) AS title,
  concat('High connection rate detected to ', dst_ip, ' in 10 seconds') AS description,
  any(src_ip) AS src_ip,
  dst_ip,
  count() AS event_count,
  'Enable DoS protection and rate limiting on affected interface' AS recommended_action
FROM tumble(cisco_observability.enhanced_asa_logs, 10s)
WHERE message_id IN ('750004', '733104', '733105')
   OR (message_category = 'Connection Tracking' AND action = 'permit')
GROUP BY
  window_start,
  dst_ip
HAVING count() >= 100;


-- Alert UDF

CREATE FUNCTION send_alert_with_webhook(message string) 
RETURNS string 
LANGUAGE PYTHON AS $$
import json
import requests

def send_alert_with_webhook(values):
    data = []
    for value in values:
        data.append(value)

    data = data[:3] # limit to first 3 items
    event = "\n".join(data)

    requests.post(
        "https://eo6fqkvuqmpqpcf.m.pipedream.net",
        data=json.dumps({
            "event": f"alert with log: {event}"
        })
    )
    return values
$$

-- DDOS Alert

CREATE ALERT cisco_observability.ddos_alert
BATCH 1 EVENTS WITH TIMEOUT 5s
LIMIT 1 ALERTS PER 15s
CALL send_alert_with_webhook
AS 
SELECT
  json_encode(*) AS message
FROM
  cisco_observability.v_alert_dos
