-- OCSF Security Sequence Analysis Queries
-- Using LAG and LAGS functions for temporal security pattern detection

-- =============================================================================
-- AUTHENTICATION SEQUENCE ANALYSIS
-- =============================================================================

-- 1. Authentication Status Change Detection
-- Detects users switching between success/failure states (potential account takeover)
SELECT 
    _tp_time,
    user_name,
    src_endpoint_ip,
    status,
    previous_status,
    src_location_country,
    previous_country,
    auth_protocol
FROM (
    SELECT 
        _tp_time,
        user_name,
        src_endpoint_ip,
        status,
        src_location_country,
        auth_protocol,
        lag(status) OVER (PARTITION BY user_name) as previous_status,
        lag(src_location_country) OVER (PARTITION BY user_name) as previous_country
    FROM ocsf.v_ocsf_authentication_flatten
)
WHERE status != previous_status AND previous_status IS NOT NULL;

-- 2. Rapid Geographic Location Changes (Impossible Travel)
-- Detects users authenticating from different countries within short time periods
SELECT 
    _tp_time,
    user_name,
    src_endpoint_ip,
    src_location_country,
    previous_country,
    previous_auth_time,
    date_diff('minute', previous_auth_time, _tp_time) as time_diff_minutes
FROM (
    SELECT 
        _tp_time,
        user_name,
        src_endpoint_ip,
        src_location_country,
        lag(src_location_country) OVER (PARTITION BY user_name) as previous_country,
        lag(_tp_time) OVER (PARTITION BY user_name) as previous_auth_time
    FROM ocsf.v_ocsf_authentication_flatten
    WHERE status = 'Success'
)
WHERE src_location_country != previous_country 
    AND previous_country IS NOT NULL
    AND date_diff('minute', previous_auth_time, _tp_time) < 120;  -- Less than 2 hours

-- 3. Failed-to-Success Authentication Pattern Analysis
-- Identifies successful logins immediately after multiple failures (potential brute force success)
SELECT 
    _tp_time,
    user_name,
    src_endpoint_ip,
    status,
    failure_sequence,
    auth_protocol
FROM (
    SELECT 
        _tp_time,
        user_name,
        src_endpoint_ip,
        status,
        auth_protocol,
        lags(status, 1, 3) OVER (PARTITION BY user_name, src_endpoint_ip) as failure_sequence
    FROM ocsf.v_ocsf_authentication_flatten
)
WHERE status = 'Success' 
    AND failure_sequence[1] = 'Failure' 
    AND failure_sequence[2] = 'Failure' 
    AND failure_sequence[3] = 'Failure';

-- 4. MFA Bypass Detection
-- Detects authentication without MFA after previous MFA usage
SELECT 
    _tp_time,
    user_name,
    src_endpoint_ip,
    is_mfa,
    previous_mfa,
    auth_protocol,
    device_name
FROM (
    SELECT 
        _tp_time,
        user_name,
        src_endpoint_ip,
        is_mfa,
        auth_protocol,
        device_name,
        lag(is_mfa) OVER (PARTITION BY user_name) as previous_mfa
    FROM ocsf.v_ocsf_authentication_flatten
    WHERE status = 'Success'
)
WHERE is_mfa = 'false' AND previous_mfa = 'true';

-- 5. Privilege Escalation in Authentication
-- Detects changes in user type during authentication sessions
SELECT 
    _tp_time,
    user_name,
    user_type,
    previous_user_type,
    src_endpoint_ip,
    auth_protocol
FROM (
    SELECT 
        _tp_time,
        user_name,
        user_type,
        src_endpoint_ip,
        auth_protocol,
        lag(user_type) OVER (PARTITION BY user_name) as previous_user_type
    FROM ocsf.v_ocsf_authentication_flatten
    WHERE status = 'Success'
)
WHERE user_type != previous_user_type 
    AND previous_user_type IS NOT NULL
    AND user_type IN ('Admin', 'Administrator', 'Root');

-- =============================================================================
-- NETWORK ACTIVITY SEQUENCE ANALYSIS
-- =============================================================================

-- 6. Data Exfiltration Volume Spike Detection
-- Identifies sudden increases in outbound traffic volume per source IP
SELECT 
    _tp_time,
    src_ip,
    dst_ip,
    traffic_bytes_out,
    previous_bytes,
    (traffic_bytes_out / previous_bytes) as bytes_increase_ratio,
    protocol_name
FROM (
    SELECT 
        _tp_time,
        src_ip,
        dst_ip,
        traffic_bytes_out,
        protocol_name,
        lag(traffic_bytes_out) OVER (PARTITION BY src_ip) as previous_bytes
    FROM ocsf.v_ocsf_network_activity_flatten
    WHERE direction = 'Outbound' AND traffic_bytes_out > 0
)
WHERE previous_bytes > 0 
    AND (traffic_bytes_out / previous_bytes) > 5  -- 5x increase
    AND traffic_bytes_out > 1048576;  -- > 1MB

-- 7. Connection Pattern Changes (Potential C2 Communication)
-- Detects changes in connection destinations that might indicate C2 communication
SELECT 
    _tp_time,
    src_ip,
    dst_ip,
    dst_location_country,
    previous_dst_country,
    protocol_name,
    traffic_bytes
FROM (
    SELECT 
        _tp_time,
        src_ip,
        dst_ip,
        dst_location_country,
        protocol_name,
        traffic_bytes,
        lag(dst_location_country) OVER (PARTITION BY src_ip) as previous_dst_country
    FROM ocsf.v_ocsf_network_activity_flatten
    WHERE direction = 'Outbound'
)
WHERE dst_location_country != previous_dst_country 
    AND previous_dst_country IS NOT NULL
    AND dst_location_country NOT IN ('US', 'CA', 'GB', 'DE', 'FR');  -- Suspicious countries

-- 8. Port Scanning Sequence Detection
-- Identifies sequential port scanning activities from the same source
SELECT 
    _tp_time,
    src_ip,
    dst_ip,
    dst_port,
    port_sequence,
    protocol_name
FROM (
    SELECT 
        _tp_time,
        src_ip,
        dst_ip,
        dst_port,
        protocol_name,
        lags(dst_port, 1, 5) OVER (PARTITION BY src_ip, dst_ip) as port_sequence
    FROM ocsf.v_ocsf_network_activity_flatten
    WHERE activity_name = 'Connect'
)
WHERE length(array_distinct(port_sequence)) >= 4;  -- Scanning multiple ports

-- 9. Network Protocol Switching Detection
-- Detects unusual protocol changes that might indicate evasion techniques
SELECT 
    _tp_time,
    src_ip,
    dst_ip,
    protocol_name,
    previous_protocol,
    traffic_bytes
FROM (
    SELECT 
        _tp_time,
        src_ip,
        dst_ip,
        protocol_name,
        traffic_bytes,
        lag(protocol_name) OVER (PARTITION BY src_ip, dst_ip) as previous_protocol
    FROM ocsf.v_ocsf_network_activity_flatten
)
WHERE protocol_name != previous_protocol 
    AND previous_protocol IS NOT NULL
    AND protocol_name IN ('UDP', 'ICMP');  -- Unusual protocols for data transfer

-- =============================================================================
-- PROCESS ACTIVITY SEQUENCE ANALYSIS
-- =============================================================================

-- 10. Process Chain Analysis (Parent-Child Relationships)
-- Tracks suspicious process execution chains
SELECT 
    _tp_time,
    device_hostname,
    process_name,
    previous_process,
    process_cmd_line,
    process_user_name
FROM (
    SELECT 
        _tp_time,
        device_hostname,
        process_name,
        process_cmd_line,
        process_user_name,
        lag(process_name) OVER (PARTITION BY device_hostname) as previous_process
    FROM ocsf.v_ocsf_process_activity_flatten
    WHERE activity_name = 'Create'
)
WHERE (previous_process = 'powershell.exe' AND process_name IN ('cmd.exe', 'rundll32.exe'))
    OR (previous_process = 'cmd.exe' AND process_name = 'powershell.exe')
    OR (previous_process = 'winword.exe' AND process_name = 'powershell.exe');

-- 11. Privilege Escalation Sequence
-- Detects user context changes in process execution indicating privilege escalation
SELECT 
    _tp_time,
    device_hostname,
    process_name,
    process_user_type,
    previous_user_type,
    process_cmd_line
FROM (
    SELECT 
        _tp_time,
        device_hostname,
        process_name,
        process_user_type,
        process_cmd_line,
        lag(process_user_type) OVER (PARTITION BY device_hostname) as previous_user_type
    FROM ocsf.v_ocsf_process_activity_flatten
    WHERE activity_name = 'Create'
)
WHERE process_user_type = 'Admin' 
    AND previous_user_type = 'User'
    AND previous_user_type IS NOT NULL;

-- 12. Living-off-the-Land Technique Detection
-- Identifies sequences of legitimate tools used for malicious purposes
SELECT 
    _tp_time,
    device_hostname,
    process_name,
    tool_sequence,
    process_user_name
FROM (
    SELECT 
        _tp_time,
        device_hostname,
        process_name,
        process_user_name,
        lags(process_name, 1, 3) OVER (PARTITION BY device_hostname) as tool_sequence
    FROM ocsf.v_ocsf_process_activity_flatten
    WHERE process_name IN ('certutil.exe', 'bitsadmin.exe', 'regsvr32.exe', 'rundll32.exe', 'powershell.exe')
        AND activity_name = 'Create'
)
WHERE length(array_distinct(tool_sequence)) >= 2;  -- Multiple different tools used

-- 13. PowerShell Command Evolution Analysis
-- Tracks evolving PowerShell commands that might indicate attack progression
SELECT 
    _tp_time,
    device_hostname,
    process_cmd_line,
    previous_cmd,
    process_user_name
FROM (
    SELECT 
        _tp_time,
        device_hostname,
        process_cmd_line,
        process_user_name,
        lag(process_cmd_line) OVER (PARTITION BY device_hostname) as previous_cmd
    FROM ocsf.v_ocsf_process_activity_flatten
    WHERE process_name = 'powershell.exe'
)
WHERE process_cmd_line != previous_cmd 
    AND previous_cmd IS NOT NULL
    AND (process_cmd_line LIKE '%ExecutionPolicy%' 
         OR process_cmd_line LIKE '%DownloadString%' 
         OR process_cmd_line LIKE '%Invoke%');

-- =============================================================================
-- SECURITY FINDINGS SEQUENCE ANALYSIS
-- =============================================================================

-- 14. Malware Family Evolution Detection
-- Tracks changes in malware classifications that might indicate attack progression
SELECT 
    _tp_time,
    resource_name,
    malware_name,
    malware_classification,
    previous_classification,
    severity,
    product_vendor_name
FROM (
    SELECT 
        _tp_time,
        resource_name,
        malware_name,
        malware_classification,
        severity,
        product_vendor_name,
        lag(malware_classification) OVER (PARTITION BY resource_name) as previous_classification
    FROM ocsf.v_ocsf_security_finding_flatten
    WHERE malware_name IS NOT NULL
)
WHERE malware_classification != previous_classification 
    AND previous_classification IS NOT NULL;

-- 15. Security Finding Severity Escalation
-- Detects increasing severity levels for the same resource
SELECT 
    _tp_time,
    resource_name,
    finding_title,
    severity,
    previous_severity,
    malware_name,
    product_vendor_name
FROM (
    SELECT 
        _tp_time,
        resource_name,
        finding_title,
        severity,
        malware_name,
        product_vendor_name,
        lag(severity) OVER (PARTITION BY resource_name) as previous_severity
    FROM ocsf.v_ocsf_security_finding_flatten
)
WHERE (severity = 'Critical' AND previous_severity IN ('High', 'Medium', 'Low'))
    OR (severity = 'High' AND previous_severity IN ('Medium', 'Low'))
    AND previous_severity IS NOT NULL;

-- =============================================================================
-- CROSS-SCHEMA SEQUENCE CORRELATION
-- =============================================================================

-- following joins wont generate results with simulator due to the limition of the simulator

-- 16. Authentication-to-Process Activity Timeline
-- Correlates authentication success with immediate suspicious process activity using ASOF JOIN
SELECT 
    a._tp_time as auth_time,
    p._tp_time as process_time,
    a.user_name,
    a.device_ip,
    p.process_name,
    p.process_cmd_line,
    date_diff('second', a._tp_time, p._tp_time) as time_diff_seconds
FROM ocsf.v_ocsf_authentication_flatten a
ASOF JOIN ocsf.v_ocsf_process_activity_flatten p 
    ON a.device_ip = p.device_ip   -- ip is randomly generated by simulator, which does not match
    AND a.user_name = p.process_user_name
    AND a._tp_time <= p._tp_time
WHERE a.status = 'Success'
    AND p.process_name IN ('powershell.exe', 'cmd.exe', 'psexec.exe'); 

-- 17. Failed Auth to Network Activity Correlation
-- Detects network scanning activities following failed authentication attempts using ASOF JOIN
SELECT 
    a._tp_time as auth_time,
    n._tp_time as scan_time,
    a.src_endpoint_ip,
    a.user_name,
    n.unique_targets,
    date_diff('minute', a._tp_time, n._tp_time) as time_diff_minutes
FROM ocsf.v_ocsf_authentication_flatten a
ASOF JOIN (
    SELECT 
        _tp_time,
        src_ip,
        count(DISTINCT dst_ip) as unique_targets
    FROM ocsf.v_ocsf_network_activity_flatten
    WHERE activity_name = 'Connect'
    GROUP BY _tp_time, src_ip
    HAVING unique_targets >= 5
) n ON a.src_endpoint_ip = n.src_ip -- ip is randomly generated by simulator, which does not match
    AND a._tp_time <= n._tp_time
WHERE a.status = 'Failure';