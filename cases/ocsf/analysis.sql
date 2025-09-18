-- OCSF Real-Time Security Analysis Queries
-- Using tumble, hop, and global aggregation for real-time threat detection

-- =============================================================================
-- AUTHENTICATION SECURITY ANALYSIS - REAL-TIME
-- =============================================================================

-- 1. Real-Time Brute Force Detection - Tumbling Windows
-- Detects multiple failed authentication attempts using 5-minute windows
SELECT 
    window_start,
    window_end,
    user_name,
    src_endpoint_ip,
    src_location_country,
    auth_protocol,
    count(*) as failed_attempts,
    count(DISTINCT dst_endpoint_ip) as target_systems
FROM tumble(ocsf.v_ocsf_authentication_flatten, 5m)
WHERE status = 'Failure'
GROUP BY window_start, window_end, user_name, src_endpoint_ip, src_location_country, auth_protocol
HAVING failed_attempts >= 3;

-- 2. Continuous Failed Authentication Counter - Global Aggregation
-- Real-time counter of failed authentication attempts by user and source
SELECT 
    user_name,
    src_endpoint_ip,
    src_location_country,
    count(*) as total_failed_attempts
FROM ocsf.v_ocsf_authentication_flatten
WHERE status = 'Failure'
GROUP BY user_name, src_endpoint_ip, src_location_country;

-- 3. Authentication Success Rate Monitoring - Hopping Windows
-- Monitors authentication patterns with 30-second steps, 2-minute windows
SELECT 
    window_start,
    window_end,
    user_name,
    auth_protocol,
    count(*) as total_attempts,
    sum(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as successful_attempts,
    sum(CASE WHEN status = 'Failure' THEN 1 ELSE 0 END) as failed_attempts,
    round(100.0 * sum(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) / count(*), 2) as success_rate_percent
FROM hop(ocsf.v_ocsf_authentication_flatten, 30s, 2m)
GROUP BY window_start, window_end, user_name, auth_protocol
HAVING total_attempts >= 5 AND success_rate_percent < 50;

-- 4. Privileged Account Activity - Tumbling Windows
-- Monitors admin account usage every 3 minutes
SELECT 
    window_start,
    window_end,
    user_name,
    user_type,
    src_endpoint_ip,
    src_location_country,
    count(*) as admin_logins,
    count(DISTINCT device_name) as unique_devices,
    sum(CASE WHEN is_mfa = 'false' THEN 1 ELSE 0 END) as non_mfa_logins
FROM tumble(ocsf.v_ocsf_authentication_flatten, 3m)
WHERE user_type IN ('Admin', 'Administrator', 'Root')
GROUP BY window_start, window_end, user_name, user_type, src_endpoint_ip, src_location_country;

-- 5. Geographic Authentication Anomalies - Tumbling Windows
-- Detects authentication from multiple countries per user in 15-minute windows
SELECT 
    window_start,
    window_end,
    user_name,
    count(DISTINCT src_location_country) as unique_countries,
    group_array(DISTINCT src_location_country) as countries,
    count(*) as total_auths,
    count(DISTINCT src_endpoint_ip) as unique_ips
FROM tumble(ocsf.v_ocsf_authentication_flatten, 15m)
WHERE status = 'Success'
GROUP BY window_start, window_end, user_name
HAVING unique_countries >= 2;

-- =============================================================================
-- NETWORK SECURITY ANALYSIS - REAL-TIME  
-- =============================================================================

-- 6. High-Severity Network Events - Global Aggregation
-- Continuous monitoring of critical network events by source and destination
SELECT 
    src_ip,
    dst_ip,
    protocol_name,
    severity,
    count(*) as event_count,
    sum(traffic_bytes) as total_bytes,
    count(DISTINCT activity_name) as unique_activities
FROM ocsf.v_ocsf_network_activity_flatten
WHERE severity IN ('High', 'Critical')
GROUP BY src_ip, dst_ip, protocol_name, severity;

-- 7. Data Exfiltration Detection - Hopping Windows
-- Sliding window analysis for large outbound transfers (1m steps, 5m windows)
SELECT 
    window_start,
    window_end,
    src_ip,
    dst_ip,
    dst_location_country,
    protocol_name,
    sum(traffic_bytes_out) as total_bytes_out,
    count(*) as connection_count,
    avg(traffic_bytes_out) as avg_bytes_per_connection
FROM hop(ocsf.v_ocsf_network_activity_flatten, 1m, 5m)
WHERE direction = 'Outbound' AND traffic_bytes_out > 0
GROUP BY window_start, window_end, src_ip, dst_ip, dst_location_country, protocol_name
HAVING total_bytes_out > 10485760;  -- > 10MB

-- 8. Network Scanning Detection - Tumbling Windows
-- Detects potential port scanning using 2-minute windows
SELECT 
    window_start,
    window_end,
    src_ip,
    protocol_name,
    count(*) as connection_attempts,
    count(DISTINCT dst_ip) as unique_targets,
    count(DISTINCT dst_location_country) as target_countries,
    avg(traffic_bytes) as avg_bytes_per_connection
FROM tumble(ocsf.v_ocsf_network_activity_flatten, 2m)
GROUP BY window_start, window_end, src_ip, protocol_name
HAVING connection_attempts >= 20 AND unique_targets >= 10;

-- 9. Real-Time Traffic Volume Monitoring - Hopping Windows
-- Monitors traffic spikes with 30-second steps, 3-minute windows
SELECT 
    window_start,
    window_end,
    protocol_name,
    direction,
    sum(traffic_bytes) as total_bytes,
    sum(traffic_packets) as total_packets,
    count(*) as total_connections,
    avg(traffic_bytes) as avg_bytes_per_connection
FROM hop(ocsf.v_ocsf_network_activity_flatten, 30s, 3m)
GROUP BY window_start, window_end, protocol_name, direction
HAVING total_bytes > 104857600;  -- > 100MB

-- =============================================================================
-- PROCESS SECURITY ANALYSIS - REAL-TIME
-- =============================================================================

-- 10. Malicious Process Detection - Global Aggregation
-- Continuous monitoring for suspicious process execution by host and process
SELECT 
    device_hostname,
    process_name,
    process_user_name,
    count(*) as execution_count,
    count(DISTINCT process_cmd_line) as unique_commands,
    group_array(DISTINCT process_cmd_line) as command_samples
FROM ocsf.v_ocsf_process_activity_flatten
WHERE (
    process_cmd_line LIKE '%powershell%ExecutionPolicy Bypass%'
    OR process_cmd_line LIKE '%cmd.exe /c%'
    OR process_cmd_line LIKE '%rundll32%'
    OR process_name IN ('psexec.exe', 'mimikatz.exe', 'procdump.exe')
)
GROUP BY device_hostname, process_name, process_user_name;

-- 11. Process Creation Rate Monitoring - Hopping Windows
-- Detects process creation bursts with 15-second steps, 1-minute windows
SELECT 
    window_start,
    window_end,
    device_hostname,
    device_ip,
    process_user_name,
    count(*) as process_count,
    count(DISTINCT process_name) as unique_processes,
    count(DISTINCT actor_process_name) as unique_parents
FROM hop(ocsf.v_ocsf_process_activity_flatten, 15s, 1m)
WHERE activity_name = 'Create'
GROUP BY window_start, window_end, device_hostname, device_ip, process_user_name
HAVING process_count >= 25;  -- for simulated data, such count can be very small as 1

-- 12. Privilege Escalation Detection - Tumbling Windows
-- Monitors privilege changes every 5 minutes
SELECT 
    window_start,
    window_end,
    device_hostname,
    process_user_name,
    process_user_type,
    actor_user_name,
    actor_user_type,
    count(*) as privilege_changes,
    count(DISTINCT process_name) as affected_processes
FROM tumble(ocsf.v_ocsf_process_activity_flatten, 5m)
WHERE process_user_type != actor_user_type
    AND (process_user_type = 'Admin' OR actor_user_type = 'Admin')
GROUP BY window_start, window_end, device_hostname, process_user_name, process_user_type, actor_user_name, actor_user_type;

-- 13. PowerShell Activity Monitoring - Tumbling Windows
-- Tracks cmd.exe usage patterns every 3 minutes
SELECT 
    window_start,
    window_end,
    device_hostname,
    process_user_name,
    count(*) as powershell_executions,
    count(DISTINCT process_cmd_line) as unique_commands,
    sum(CASE WHEN process_cmd_line LIKE '%ExecutionPolicy%' THEN 1 ELSE 0 END) as bypass_attempts,
    sum(CASE WHEN process_cmd_line LIKE '%encoded%' THEN 1 ELSE 0 END) as encoded_commands
FROM tumble(ocsf.v_ocsf_process_activity_flatten, 3m)
WHERE process_name = 'cmd.exe'
GROUP BY window_start, window_end, device_hostname, process_user_name;

-- =============================================================================
-- SECURITY FINDINGS ANALYSIS - REAL-TIME
-- =============================================================================

-- 14. Critical Security Findings - Tumbling Windows
-- Monitors high-priority detections every minute
SELECT 
    window_start,
    window_end,
    severity,
    product_vendor_name,
    malware_classification,
    count(*) as finding_count,
    count(DISTINCT malware_name) as unique_malware,
    count(DISTINCT resource_name) as affected_resources,
    group_array(DISTINCT finding_title) as finding_titles
FROM tumble(ocsf.v_ocsf_security_finding_flatten, 1m)
WHERE severity IN ('High', 'Critical')
GROUP BY window_start, window_end, severity, product_vendor_name, malware_classification;

-- 15. Malware Detection Trends - Hopping Windows
-- Analyzes malware patterns with 2-minute steps, 10-minute windows
SELECT 
    window_start,
    window_end,
    malware_name,
    malware_classification,
    count(*) as detection_count,
    count(DISTINCT resource_name) as affected_resources,
    count(DISTINCT product_vendor_name) as detection_sources,
    avg(CASE WHEN severity = 'Critical' THEN 4 WHEN severity = 'High' THEN 3 WHEN severity = 'Medium' THEN 2 ELSE 1 END) as avg_severity_score
FROM hop(ocsf.v_ocsf_security_finding_flatten, 2m, 10m)
WHERE malware_name IS NOT NULL
GROUP BY window_start, window_end, malware_name, malware_classification
HAVING detection_count >= 2;

-- 16. Real-Time Malware Counter - Global Aggregation
-- Continuous count of malware detections by classification and name
SELECT 
    malware_classification,
    malware_name,
    count(*) as total_detections,
    count(DISTINCT resource_name) as unique_resources,
    count(DISTINCT product_vendor_name) as detection_sources
FROM ocsf.v_ocsf_security_finding_flatten
WHERE malware_name IS NOT NULL
GROUP BY malware_classification, malware_name;

-- =============================================================================
-- REAL-TIME SECURITY DASHBOARDS
-- =============================================================================

-- 17. Authentication Dashboard - Tumbling Windows (1 minute)
SELECT 
    window_start,
    window_end,
    count(*) as total_auth_events,
    sum(CASE WHEN status = 'Failure' THEN 1 ELSE 0 END) as failed_auths,
    sum(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as successful_auths,
    sum(CASE WHEN user_type IN ('Admin', 'Administrator') THEN 1 ELSE 0 END) as admin_auths,
    sum(CASE WHEN is_mfa = 'false' THEN 1 ELSE 0 END) as non_mfa_auths,
    count(DISTINCT user_name) as unique_users,
    count(DISTINCT src_endpoint_ip) as unique_source_ips
FROM tumble(ocsf.v_ocsf_authentication_flatten, 1m)
GROUP BY window_start, window_end;

-- 18. Network Dashboard - Tumbling Windows (1 minute)
SELECT 
    window_start,
    window_end,
    count(*) as total_network_events,
    sum(CASE WHEN severity IN ('High', 'Critical') THEN 1 ELSE 0 END) as high_severity_events,
    sum(CASE WHEN direction = 'Outbound' THEN 1 ELSE 0 END) as outbound_connections,
    sum(CASE WHEN direction = 'Inbound' THEN 1 ELSE 0 END) as inbound_connections,
    sum(traffic_bytes) as total_bytes_transferred,
    count(DISTINCT src_ip) as unique_source_ips,
    count(DISTINCT dst_ip) as unique_dest_ips
FROM tumble(ocsf.v_ocsf_network_activity_flatten, 1m)
GROUP BY window_start, window_end;

-- 19. Process Dashboard - Tumbling Windows (1 minute)
SELECT 
    window_start,
    window_end,
    count(*) as total_process_events,
    sum(CASE WHEN activity_name = 'Create' THEN 1 ELSE 0 END) as process_creations,
    sum(CASE WHEN process_user_type = 'Admin' THEN 1 ELSE 0 END) as admin_processes,
    sum(CASE WHEN process_name IN ('powershell.exe', 'cmd.exe') THEN 1 ELSE 0 END) as shell_processes,
    count(DISTINCT device_hostname) as unique_hosts,
    count(DISTINCT process_user_name) as unique_users,
    count(DISTINCT process_name) as unique_process_names
FROM tumble(ocsf.v_ocsf_process_activity_flatten, 1m)
GROUP BY window_start, window_end;

-- 20. Security Findings Dashboard - Tumbling Windows (1 minute)
SELECT 
    window_start,
    window_end,
    count(*) as total_findings,
    sum(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical_findings,
    sum(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) as high_findings,
    sum(CASE WHEN malware_name IS NOT NULL THEN 1 ELSE 0 END) as malware_detections,
    count(DISTINCT malware_classification) as unique_malware_types,
    count(DISTINCT product_vendor_name) as detection_sources,
    count(DISTINCT resource_name) as affected_resources
FROM tumble(ocsf.v_ocsf_security_finding_flatten, 1m)
GROUP BY window_start, window_end;

-- =============================================================================
-- GLOBAL AGGREGATION QUERIES - CONTINUOUS COUNTERS
-- =============================================================================

-- 21. Live Security Metrics Overview
SELECT 
    'Authentication Failures' as metric_name,
    count(*) as current_count
FROM ocsf.v_ocsf_authentication_flatten
WHERE status = 'Failure'

UNION ALL

SELECT 
    'High Severity Network Events' as metric_name,
    count(*) as current_count
FROM ocsf.v_ocsf_network_activity_flatten
WHERE severity IN ('High', 'Critical')

UNION ALL

SELECT 
    'Suspicious Process Executions' as metric_name,
    count(*) as current_count
FROM ocsf.v_ocsf_process_activity_flatten
WHERE process_name IN ('powershell.exe', 'cmd.exe', 'psexec.exe')

UNION ALL

SELECT 
    'Critical Security Findings' as metric_name,
    count(*) as current_count
FROM ocsf.v_ocsf_security_finding_flatten
WHERE severity = 'Critical';

-- 22. Top Threat Actors - Global Aggregation
-- Identifies most active threat sources by failed authentication patterns
SELECT 
    src_endpoint_ip,
    src_location_country,
    count(*) as failed_auth_attempts,
    count(DISTINCT user_name) as targeted_users,
    count(DISTINCT dst_endpoint_ip) as targeted_systems
FROM ocsf.v_ocsf_authentication_flatten
WHERE status = 'Failure'
GROUP BY src_endpoint_ip, src_location_country;

-- 23. Active Malware Families - Global Aggregation
-- Tracks current malware activity by classification and family
SELECT 
    malware_classification,
    malware_name,
    count(*) as active_detections,
    count(DISTINCT resource_name) as infected_resources,
    group_array(DISTINCT product_vendor_name) as detecting_products
FROM ocsf.v_ocsf_security_finding_flatten
WHERE malware_name IS NOT NULL
GROUP BY malware_classification, malware_name;