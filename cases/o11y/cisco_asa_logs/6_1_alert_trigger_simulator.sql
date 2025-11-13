
CREATE RANDOM STREAM cisco_asa_simulator.cisco_asa_critical_logs (
    -- Timestamp
    timestamp datetime64(3) DEFAULT now64(3),
    
    -- Device identifier (asa-fw01 to asa-fw25)
    device_name string DEFAULT concat('asa-fw', lpad(to_string((rand(1) % 26) + 1), 2, '0')),
    
    -- Failover unit type
    unit_type string DEFAULT array_element(['Primary', 'Secondary'], (rand(2) % 2) + 1),
    
    -- Message category (all alerts)
    message_category string DEFAULT 'alert',
    
    -- Message ID list (ONLY severity 1 messages)
    message_id string DEFAULT array_element([
        -- ========== SEVERITY 1 - ALERT (Failover/HA) ==========
        '101001',  -- Failover cable OK
        '101002',  -- Bad failover cable
        '103002',  -- Other firewall network interface OK
        '104004',  -- Switching to OK
        '104500',  -- Switching to ACTIVE (cause)
        '104502',  -- Becoming Backup unit failed
        '105003',  -- Monitoring on interface waiting
        '105004',  -- Monitoring on interface normal
        '106022',  -- Deny TCP connection spoof
        '106101',  -- ACL log flows reached limit
        '107001'   -- RIP authentication failed
    ], (rand(5) % 11) + 1),
    
    -- Severity level (always 1 for this stream)
    severity int8 DEFAULT 1,
    
    -- Source IP addresses (realistic distribution)
    src_ip string DEFAULT multi_if(
        (rand(6) % 100) <= 60, concat('10.', to_string((rand(7) % 256)), '.', to_string((rand(8) % 256)), '.', to_string((rand(9) % 256))),
        (rand(10) % 100) <= 80, concat('192.168.', to_string((rand(11) % 256)), '.', to_string((rand(12) % 256))),
        (rand(13) % 100) <= 90, concat('172.', to_string((rand(14) % 16) + 16), '.', to_string((rand(15) % 256)), '.', to_string((rand(16) % 256))),
        concat(to_string((rand(17) % 223) + 1), '.', to_string((rand(18) % 256)), '.', to_string((rand(19) % 256)), '.', to_string((rand(20) % 256)))
    ),
    
    -- Destination IP addresses
    dst_ip string DEFAULT multi_if(
        (rand(21) % 100) <= 40, concat('10.', to_string((rand(22) % 256)), '.', to_string((rand(23) % 256)), '.', to_string((rand(24) % 256))),
        (rand(25) % 100) <= 55, concat('192.168.', to_string((rand(26) % 256)), '.', to_string((rand(27) % 256))),
        (rand(28) % 100) <= 65, concat('172.', to_string((rand(29) % 16) + 16), '.', to_string((rand(30) % 256)), '.', to_string((rand(31) % 256))),
        concat(to_string((rand(32) % 223) + 1), '.', to_string((rand(33) % 256)), '.', to_string((rand(34) % 256)), '.', to_string((rand(35) % 256)))
    ),
    
    -- Source port
    src_port uint16 DEFAULT multi_if(
        (rand(36) % 100) <= 70, (rand(37) % 30000) + 32768,
        (rand(38) % 65535) + 1
    ),
    
    -- Destination port (weighted towards services)
    dst_port uint16 DEFAULT multi_if(
        (rand(39) % 100) <= 30, 443,
        (rand(40) % 100) <= 50, 80,
        (rand(41) % 100) <= 65, 22,
        (rand(42) % 100) <= 75, 3389,
        (rand(43) % 100) <= 85, 53,
        (rand(44) % 100) <= 90, 25,
        (rand(45) % 65535) + 1
    ),
    
    -- Protocol
    protocol string DEFAULT array_element(['TCP', 'UDP', 'ICMP', 'ESP'], multi_if(
        (rand(46) % 100) <= 70, 1,
        (rand(47) % 100) <= 90, 2,
        (rand(48) % 100) <= 97, 3,
        4
    )),
    
    -- Interface names
    src_interface string DEFAULT array_element(['outside', 'inside', 'dmz', 'management', 'wan', 'lan', 'failover'], (rand(49) % 7) + 1),
    dst_interface string DEFAULT array_element(['outside', 'inside', 'dmz', 'management', 'wan', 'lan', 'failover'], (rand(50) % 7) + 1),
    
    -- Connection ID
    connection_id uint32 DEFAULT rand(51),
    
    -- TCP flags
    tcp_flags string DEFAULT array_element([
        'TCP FINs', 'TCP RSTs', 'TCP SYNs', 'TCP data', 'SYN ACK', 'FIN ACK'
    ], (rand(52) % 6) + 1),
    
    -- Connection direction
    direction string DEFAULT array_element(['Inbound', 'Outbound'], (rand(53) % 2) + 1),
    
    -- ACL name
    acl_name string DEFAULT array_element([
        'INSIDE_OUT', 'OUTSIDE_IN', 'DMZ_ACCESS', 'MANAGEMENT', 
        'VPN_ACCESS', 'DEFAULT_POLICY', 'INTERNET_ACCESS', 'ADMIN_ACL'
    ], (rand(54) % 8) + 1),
    
    -- Error code
    error_code string DEFAULT concat('0x', hex((rand(55) % 65535))),
    
    -- Failover reason
    failover_reason string DEFAULT array_element([
        'health check failed',
        'interface down',
        'manual switch',
        'configuration sync failed',
        'cable disconnect',
        'peer unreachable',
        'unit failure',
        'operator initiated'
    ], (rand(56) % 8) + 1),
    
    -- ACL flow limit
    acl_flow_limit uint32 DEFAULT (rand(57) % 5000) + 5000,
    
    -- RIP sequence
    rip_sequence uint32 DEFAULT rand(58) % 10000,
    
    -- Priority (syslog priority = facility * 8 + severity)
    -- Cisco ASA uses facility 23, so priority = 184 + 1 = 185
    priority uint8 DEFAULT 185,
    
    -- Message text construction based on message_id
    message_text string DEFAULT (
        multi_if(
            -- ========== SEVERITY 1 - FAILOVER MESSAGES ==========
            message_id = '101001', concat('(', unit_type, ') Failover cable OK.'),
            
            message_id = '101002', concat('(', unit_type, ') Bad failover cable.'),
            
            message_id = '103002', concat(
                '(', unit_type, ') Other firewall network interface ', src_interface, ' OK.'
            ),
            
            message_id = '104004', concat('(', unit_type, ') Switching to OK.'),
            
            message_id = '104500', concat(
                '(', unit_type, ') Switching to ACTIVE (cause: ', failover_reason, ')'
            ),
            
            message_id = '104502', concat('(', unit_type, ') Becoming Backup unit failed.'),
            
            message_id = '105003', concat(
                '(', unit_type, ') Monitoring on interface ', src_interface, ' waiting'
            ),
            
            message_id = '105004', concat(
                '(', unit_type, ') Monitoring on interface ', src_interface, ' normal'
            ),
            
            -- ========== SEVERITY 1 - SPOOFING/SECURITY ALERTS ==========
            message_id = '106022', concat(
                'Deny ', lower(protocol), ' connection spoof from ', src_ip, ' to ', dst_ip,
                ' on interface ', src_interface
            ),
            
            message_id = '106101', concat(
                'The number of ACL log flows has reached limit (', to_string(acl_flow_limit), ')'
            ),
            
            message_id = '107001', concat(
                'RIP auth failed from ', src_ip, ': version=2, type=md5, mode=', 
                array_element(['text', 'md5'], (rand(59) % 2) + 1), 
                ', sequence=', to_string(rip_sequence), ' on interface ', src_interface
            ),
            
            -- Default fallback
            concat('Alert event for message ID ', message_id, ' on device ', device_name)
        )
    ),
    
    -- Final log message in Cisco ASA syslog format
    log_message string DEFAULT concat(
        '<', to_string(priority), '>',
        format_datetime(timestamp, '%b %e %H:%M:%S'),
        ' ', device_name,
        ' %ASA-', to_string(severity), '-', message_id, ': ',
        message_text
    ),
    
    -- Additional flags for easy filtering
    is_failover_issue bool DEFAULT message_id IN (
        '101001', '101002', '103002', '104004', '104500', '104502', '105003', '105004'
    ),
    
    is_spoof_alert bool DEFAULT message_id = '106022',
    
    is_routing_issue bool DEFAULT message_id = '107001',
    
    is_acl_limit bool DEFAULT message_id = '106101'
    
) SETTINGS eps = 0.5;  -- 1 critical issue per two seconds

-- Materialized sending failover cable error logs
CREATE MATERIALIZED VIEW cisco_asa_simulator.mv_asa_critical_logs
INTO cisco_observability.asa_logs_stream 
AS
SELECT
    log_message AS message
FROM cisco_asa_simulator.cisco_asa_critical_logs;

SYSTEM PAUSE MATERIALIZED VIEW cisco_asa_simulator.mv_asa_critical_logs;
SYSTEM RESUME MATERIALIZED VIEW cisco_asa_simulator.mv_asa_critical_logs;