-- Critical Security Events (Immediate)
CREATE VIEW cisco_observability.v_alert_critical_events
AS
SELECT
  now64(3) AS alert_time,
  concat('CRIT-', to_string(uuid())) AS alert_id,
  'Critical Security Event' AS alert_type,
  device_name,
  concat('Critical Event: Message ID ', message_id) AS title,
  concat(
    'Device ', device_name, ' reported a critical event (Severity ', to_string(severity), '). ',
    'Message: ', asa_message
  ) AS description,
  if_null(to_string(src_ip), 'N/A') AS src_ip,
  if_null(to_string(dst_ip), 'N/A') AS dst_ip,
  1 AS event_count,
  [asa_message] AS raw_events,
  multi_if(
    message_id = '106022', 'Investigate connection spoofing attempt - possible attack',
    message_id = '108003', 'Block source IP - SMTP malicious pattern detected',
    message_id = '202010', 'Review NAT pool configuration - capacity issue',
    message_id = '702307', 'Expand NAT pool - exhaustion detected',
    'Investigate immediately and review security posture'
  ) AS recommended_action
FROM cisco_observability.flatten_extracted_asa_logs
WHERE severity <= 2;  -- Critical (0), Alert (1), Critical (2)

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
  username AS dst_ip,  -- Use username as "target" (for consistency with alert schema)
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
  'CRITICAL' AS severity,
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

-- check the alert @ https://pipedream.com/@timeplus/projects/proj_e5sjxDe/timeplus-alert-demo-p_ZJCK6Rw/inspect
