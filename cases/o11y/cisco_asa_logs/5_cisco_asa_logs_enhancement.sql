-- Enhanced stream with enriched context and derived fields
CREATE STREAM cisco.enhanced_asa_logs
(
  `ingestion_time` datetime64(3),
  `log_timestamp` string,
  `device_name` string,
  `severity` nullable(int8),
  `severity_label` string,
  `message_id` string,
  `message_category` string,
  `threat_level` string,
  `asa_message` string,
  
  -- Extracted network fields
  `src_ip` nullable(string),
  `dst_ip` nullable(string),
  `src_port` nullable(uint16),
  `dst_port` nullable(uint16),
  `protocol` nullable(string),
  `src_interface` nullable(string),
  `dst_interface` nullable(string),
  
  -- Enriched fields
  `is_internal_src` bool,
  `is_internal_dst` bool,
  `is_internal_to_internal` bool,
  `is_external_to_internal` bool,
  `is_suspicious_port` nullable(bool),
  `is_high_risk_protocol` nullable(bool),
  
  -- Contextual enrichment
  `is_business_hours` bool,
  `day_of_week` string,
  `traffic_direction` string,
  
  -- Threat indicators
  `is_critical` nullable(bool),
  `requires_investigation` nullable(bool),
  `action` nullable(string),
  
  `raw_message` string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192 , logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

CREATE MATERIALIZED VIEW cisco.mv_enhance_asa_logs
INTO cisco.enhanced_asa_logs AS
WITH extracted AS (
  SELECT
    *,
    -- Extract IPs
    extract(asa_message, 'from ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS src_ip,
    extract(asa_message, 'to ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})') AS dst_ip,
    -- Extract ports
    to_uint16_or_null(extract(asa_message, 'from [0-9.]+/(\\d+)')) AS src_port,
    to_uint16_or_null(extract(asa_message, 'to [0-9.]+/(\\d+)')) AS dst_port,
    -- Extract protocol
    upper(extract(asa_message, '(TCP|UDP|ICMP|ESP|AH|GRE)')) AS protocol,
    -- Extract interfaces
    extract(asa_message, 'from (\\w+):') AS src_interface,
    extract(asa_message, 'to (\\w+):') AS dst_interface,
    -- Extract action
    multi_if(
      position(lower(asa_message), 'denied') > 0, 'deny',
      position(lower(asa_message), 'deny ') > 0, 'deny',
      position(lower(asa_message), 'permitted') > 0, 'permit',
      position(lower(asa_message), 'built') > 0, 'permit',
      NULL
    ) AS action
  FROM cisco.parsed_asa_logs
)
SELECT
  ingestion_time,
  log_timestamp,
  device_name,
  severity,
  
  -- Severity label enrichment
  multi_if(
    severity = 1, 'ALERT',
    severity = 2, 'CRITICAL',
    severity = 3, 'ERROR',
    severity = 4, 'WARNING',
    severity = 5, 'NOTIFICATION',
    severity = 6, 'INFORMATIONAL',
    severity = 7, 'DEBUG',
    'UNKNOWN'
  ) AS severity_label,
  
  message_id,
  
  -- Message category enrichment
  multi_if(
    message_id LIKE '302%', 'Connection Tracking',
    message_id LIKE '305%', 'State Tracking',
    message_id LIKE '106%', 'Access Control',
    message_id LIKE '313%', 'ICMP',
    message_id LIKE '109%', 'Authentication',
    message_id LIKE '101%' OR message_id LIKE '103%' OR message_id LIKE '104%' OR message_id LIKE '105%', 'Failover/HA',
    message_id LIKE '733%', 'Threat Detection',
    message_id LIKE '750%', 'DoS Protection',
    message_id LIKE '602%' OR message_id LIKE '702%', 'NAT',
    message_id LIKE '400%', 'IPS',
    message_id IN ('107001', '108003', '202010'), 'Security Error',
    'Other'
  ) AS message_category,
  
  -- Threat level assessment
  multi_if(
    severity <= 2, 'CRITICAL',
    severity = 3, 'HIGH',
    severity = 4 AND message_id IN ('106023', '106015', '733102', '733104', '733105'), 'MEDIUM',
    message_id IN ('106022', '106101', '108003', '202010', '702307'), 'HIGH',
    message_id LIKE '750%', 'MEDIUM',
    'LOW'
  ) AS threat_level,
  
  asa_message,
  
  -- Network fields
  src_ip,
  dst_ip,
  src_port,
  dst_port,
  protocol,
  src_interface,
  dst_interface,
  
  -- Internal network detection (RFC1918)
  multi_if(
    src_ip LIKE '10.%', true,
    src_ip LIKE '192.168.%', true,
    match(src_ip, '^172\\.(1[6-9]|2[0-9]|3[0-1])\\.'), true,
    false
  ) AS is_internal_src,
  
  multi_if(
    dst_ip LIKE '10.%', true,
    dst_ip LIKE '192.168.%', true,
    match(dst_ip, '^172\\.(1[6-9]|2[0-9]|3[0-1])\\.'), true,
    false
  ) AS is_internal_dst,
  
  -- Traffic flow analysis
  is_internal_src AND is_internal_dst AS is_internal_to_internal,
  NOT is_internal_src AND is_internal_dst AS is_external_to_internal,
  
  -- Suspicious port detection
  dst_port IN (23, 135, 139, 445, 1433, 3306, 3389, 5432, 5900, 6379, 27017) AS is_suspicious_port,
  
  -- High-risk protocol detection
  protocol IN ('ICMP', 'GRE') OR dst_port IN (21, 23, 69, 161) AS is_high_risk_protocol,
  
  -- Business hours detection (assuming UTC, adjust as needed)
  hour(now()) >= 8 AND hour(now()) < 18 AS is_business_hours,
  
  -- Day of week
  to_day_of_week(now()) AS day_of_week,
  
  -- Traffic direction labeling
  multi_if(
    is_internal_to_internal, 'internal-to-internal',
    is_external_to_internal, 'inbound',
    is_internal_src AND NOT is_internal_dst, 'outbound',
    'external-to-external'
  ) AS traffic_direction,
  
  -- Critical event flag
  (severity <= 2) OR 
  (message_id IN ('106022', '106101', '108003', '202010', '702307', '733102')) OR
  (is_external_to_internal AND action = 'deny' AND is_suspicious_port) AS is_critical,
  
  -- Investigation flag
  (severity <= 3) OR
  (action = 'deny' AND is_external_to_internal) OR
  (message_id LIKE '733%') OR
  (message_id LIKE '750%') AS requires_investigation,
  
  action,
  raw_message
FROM extracted;


-- Mutable stream for device/asset information
CREATE MUTABLE STREAM cisco.device_assets
(
  `device_name` string,
  `hostname` string,
  `location` string,
  `datacenter` string,
  `rack_position` string,
  `device_type` string,
  `hardware_model` string,
  `software_version` string,
  `management_ip` string,
  `owner_team` string,
  `criticality` string,
  `deployment_date` datetime,
  `maintenance_window` string,
  `last_updated` datetime64(3) DEFAULT now64(3)
)
PRIMARY KEY device_name;

-- Insert sample device data
-- Insert device assets for FW00 to FW30
INSERT INTO cisco.device_assets (device_name, hostname, location, datacenter, rack_position, device_type, hardware_model, software_version, management_ip, owner_team, criticality, deployment_date, maintenance_window) VALUES
('FW00', 'fw-dc1-edge-01', 'US-East', 'DC1-NewYork', 'R12-U15', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.10.1.10', 'Network-Security', 'Critical', '2022-01-15', 'Sun 02:00-06:00'),
('FW01', 'fw-dc1-edge-02', 'US-East', 'DC1-NewYork', 'R12-U16', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.10.1.11', 'Network-Security', 'Critical', '2022-01-15', 'Sun 02:00-06:00'),
('FW02', 'fw-dc1-dmz-01', 'US-East', 'DC1-NewYork', 'R15-U10', 'DMZ Firewall', 'ASA-5545-X', '9.16.2', '10.10.2.10', 'Network-Security', 'High', '2021-08-20', 'Sat 22:00-02:00'),
('FW03', 'fw-dc1-dmz-02', 'US-East', 'DC1-NewYork', 'R15-U11', 'DMZ Firewall', 'ASA-5545-X', '9.16.2', '10.10.2.11', 'Network-Security', 'High', '2021-08-20', 'Sat 22:00-02:00'),
('FW04', 'fw-dc1-internal-01', 'US-East', 'DC1-NewYork', 'R18-U05', 'Internal Firewall', 'ASA-5525-X', '9.14.3', '10.10.3.10', 'Network-Security', 'Medium', '2020-11-10', 'Sat 23:00-03:00'),
('FW05', 'fw-dc2-edge-01', 'US-West', 'DC2-SanFrancisco', 'R08-U12', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.20.1.10', 'Network-Security', 'Critical', '2022-03-10', 'Sun 02:00-06:00'),
('FW06', 'fw-dc2-edge-02', 'US-West', 'DC2-SanFrancisco', 'R08-U13', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.20.1.11', 'Network-Security', 'Critical', '2022-03-10', 'Sun 02:00-06:00'),
('FW07', 'fw-dc2-internal-01', 'US-West', 'DC2-SanFrancisco', 'R10-U05', 'Internal Firewall', 'ASA-5525-X', '9.14.3', '10.20.2.10', 'Network-Security', 'Medium', '2020-11-05', 'Sat 22:00-02:00'),
('FW08', 'fw-dc2-dmz-01', 'US-West', 'DC2-SanFrancisco', 'R11-U08', 'DMZ Firewall', 'ASA-5545-X', '9.16.2', '10.20.3.10', 'Network-Security', 'High', '2021-09-12', 'Sun 01:00-05:00'),
('FW09', 'fw-dc3-edge-01', 'EU-West', 'DC3-London', 'R05-U20', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.30.1.10', 'Network-EMEA', 'Critical', '2022-06-01', 'Sun 01:00-05:00'),
('FW10', 'fw-dc3-edge-02', 'EU-West', 'DC3-London', 'R05-U21', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.30.1.11', 'Network-EMEA', 'Critical', '2022-06-01', 'Sun 01:00-05:00'),
('FW11', 'fw-dc3-dmz-01', 'EU-West', 'DC3-London', 'R07-U12', 'DMZ Firewall', 'ASA-5545-X', '9.16.2', '10.30.2.10', 'Network-EMEA', 'High', '2021-10-15', 'Sat 22:00-02:00'),
('FW12', 'fw-dc3-internal-01', 'EU-West', 'DC3-London', 'R09-U06', 'Internal Firewall', 'ASA-5525-X', '9.14.3', '10.30.3.10', 'Network-EMEA', 'Medium', '2020-12-08', 'Sun 02:00-06:00'),
('FW13', 'fw-dc4-edge-01', 'APAC', 'DC4-Singapore', 'R03-U08', 'Edge Firewall', 'ASA-5545-X', '9.16.2', '10.40.1.10', 'Network-APAC', 'Critical', '2021-12-15', 'Sun 03:00-07:00'),
('FW14', 'fw-dc4-edge-02', 'APAC', 'DC4-Singapore', 'R03-U09', 'Edge Firewall', 'ASA-5545-X', '9.16.2', '10.40.1.11', 'Network-APAC', 'Critical', '2021-12-15', 'Sun 03:00-07:00'),
('FW15', 'fw-dc4-dmz-01', 'APAC', 'DC4-Singapore', 'R03-U15', 'DMZ Firewall', 'ASA-5525-X', '9.14.3', '10.40.2.10', 'Network-APAC', 'High', '2021-05-20', 'Sat 23:00-03:00'),
('FW16', 'fw-dc5-edge-01', 'EU-Central', 'DC5-Frankfurt', 'R06-U18', 'Edge Firewall', 'ASA-5555-X', '9.16.4', '10.50.1.10', 'Network-EMEA', 'Critical', '2022-04-20', 'Sun 01:00-05:00'),
('FW17', 'fw-dc5-dmz-01', 'EU-Central', 'DC5-Frankfurt', 'R08-U10', 'DMZ Firewall', 'ASA-5545-X', '9.16.2', '10.50.2.10', 'Network-EMEA', 'High', '2022-04-20', 'Sun 01:00-05:00'),
('FW18', 'fw-dc6-edge-01', 'APAC', 'DC6-Sydney', 'R04-U12', 'Edge Firewall', 'ASA-5545-X', '9.16.2', '10.60.1.10', 'Network-APAC', 'Critical', '2022-07-10', 'Sun 04:00-08:00'),
('FW19', 'fw-dc6-internal-01', 'APAC', 'DC6-Sydney', 'R05-U08', 'Internal Firewall', 'ASA-5525-X', '9.14.3', '10.60.2.10', 'Network-APAC', 'Medium', '2021-03-25', 'Sun 05:00-09:00'),
('FW20', 'fw-branch-nyc-01', 'US-East', 'Branch-NYC', 'Wall-Mount-A', 'Branch Firewall', 'ASA-5506-X', '9.12.4', '10.70.1.10', 'Branch-Networks', 'Low', '2020-01-10', 'Sun 04:00-06:00'),
('FW21', 'fw-branch-chicago-01', 'US-Central', 'Branch-Chicago', 'Wall-Mount-B', 'Branch Firewall', 'ASA-5506-X', '9.12.4', '10.70.2.10', 'Branch-Networks', 'Low', '2020-02-15', 'Sun 03:00-05:00'),
('FW22', 'fw-branch-dallas-01', 'US-Central', 'Branch-Dallas', 'Wall-Mount-C', 'Branch Firewall', 'ASA-5508-X', '9.14.2', '10.70.3.10', 'Branch-Networks', 'Low', '2021-01-20', 'Sun 03:00-05:00'),
('FW23', 'fw-branch-seattle-01', 'US-West', 'Branch-Seattle', 'Wall-Mount-D', 'Branch Firewall', 'ASA-5506-X', '9.12.4', '10.70.4.10', 'Branch-Networks', 'Low', '2020-03-18', 'Sun 02:00-04:00'),
('FW24', 'fw-branch-boston-01', 'US-East', 'Branch-Boston', 'Wall-Mount-E', 'Branch Firewall', 'ASA-5508-X', '9.14.2', '10.70.5.10', 'Branch-Networks', 'Low', '2021-04-22', 'Sun 04:00-06:00'),
('FW25', 'fw-hub-miami-01', 'US-Southeast', 'Hub-Miami', 'R14-U10', 'Regional Hub', 'ASA-5545-X', '9.16.2', '10.80.1.10', 'Network-Regional', 'High', '2022-02-15', 'Sun 03:00-07:00'),
('FW26', 'fw-hub-toronto-01', 'CA-Central', 'Hub-Toronto', 'R11-U15', 'Regional Hub', 'ASA-5545-X', '9.16.2', '10.80.2.10', 'Network-Regional', 'High', '2022-05-10', 'Sun 02:00-06:00'),
('FW27', 'fw-hub-mumbai-01', 'APAC', 'Hub-Mumbai', 'R08-U20', 'Regional Hub', 'ASA-5525-X', '9.14.3', '10.80.3.10', 'Network-APAC', 'High', '2021-11-05', 'Sun 03:30-07:30'),
('FW28', 'fw-test-lab-01', 'US-East', 'DC1-NewYork', 'R20-U05', 'Test Firewall', 'ASA-5515-X', '9.16.4', '10.90.1.10', 'Engineering-QA', 'Low', '2020-06-12', 'Any Time'),
('FW29', 'fw-dr-backup-01', 'US-Central', 'DC7-Denver', 'R10-U12', 'DR Firewall', 'ASA-5555-X', '9.16.4', '10.90.2.10', 'Network-Security', 'Critical', '2022-08-01', 'Sun 02:00-06:00'),
('FW30', 'fw-dev-staging-01', 'US-West', 'DC2-SanFrancisco', 'R22-U08', 'Development Firewall', 'ASA-5515-X', '9.18.1', '10.90.3.10', 'Engineering-Dev', 'Low', '2023-01-10', 'Any Time');


CREATE VIEW cisco.v_enhance_with_assets
AS
SELECT
  -- All fields from enhanced_asa_logs
  e.ingestion_time,
  e.log_timestamp,
  e.device_name,
  e.severity,
  e.severity_label,
  e.message_id,
  e.message_category,
  e.threat_level,
  e.asa_message,
  
  -- Network fields
  e.src_ip,
  e.dst_ip,
  e.src_port,
  e.dst_port,
  e.protocol,
  e.src_interface,
  e.dst_interface,
  
  -- Enriched fields
  e.is_internal_src,
  e.is_internal_dst,
  e.is_internal_to_internal,
  e.is_external_to_internal,
  e.is_suspicious_port,
  e.is_high_risk_protocol,
  
  -- Contextual
  e.is_business_hours,
  e.day_of_week,
  e.traffic_direction,
  
  -- Threat indicators
  e.is_critical,
  e.requires_investigation,
  e.action,
  
  -- Device/Asset fields from JOIN
  a.hostname AS device_hostname,
  a.location AS device_location,
  a.datacenter AS device_datacenter,
  a.rack_position AS device_rack_position,
  a.device_type AS device_type,
  a.hardware_model AS device_hardware_model,
  a.software_version AS device_software_version,
  a.management_ip AS device_management_ip,
  a.owner_team AS device_owner_team,
  a.criticality AS device_criticality,
  a.deployment_date AS device_deployment_date,
  a.maintenance_window AS device_maintenance_window,
  
  -- Computed fields
  (a.criticality = 'Critical') AS is_critical_device,
  date_diff('day', a.deployment_date, now()) AS device_age_days,
  
  -- Check if current time is within maintenance window
  -- Simple check for "Sun 02:00-06:00" format
  (
    to_day_of_week(now()) = 
      CASE 
        WHEN a.maintenance_window LIKE 'Sun%' THEN 7
        WHEN a.maintenance_window LIKE 'Mon%' THEN 1
        WHEN a.maintenance_window LIKE 'Tue%' THEN 2
        WHEN a.maintenance_window LIKE 'Wed%' THEN 3
        WHEN a.maintenance_window LIKE 'Thu%' THEN 4
        WHEN a.maintenance_window LIKE 'Fri%' THEN 5
        WHEN a.maintenance_window LIKE 'Sat%' THEN 6
        ELSE 0
      END
    AND
    hour(now()) >= to_uint8(extract(a.maintenance_window, '(\\d{2}):\\d{2}'))
    AND
    hour(now()) < to_uint8(extract(a.maintenance_window, '-(\\d{2}):\\d{2}'))
  ) OR (a.maintenance_window = 'Any Time') AS is_in_maintenance_window,
  
  e.raw_message
FROM cisco.enhanced_asa_logs e
LEFT JOIN cisco.device_assets a ON e.device_name = a.device_name;