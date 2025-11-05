
CREATE DATABASE IF NOT EXISTS cisco;

CREATE RANDOM STREAM cisco.cisco_asa_logs (
    -- Timestamp
    timestamp datetime64(3) DEFAULT now64(3),
    
    -- Device identifier (FW00-FW25)
    device_name string DEFAULT concat('FW', lpad(to_string(rand() % 26), 2, '0')),
    
    -- Message category (for anomaly labeling)
    message_category string DEFAULT array_element([
        'informational',  -- 85% normal
        'warning',        -- 10% warnings
        'error',          -- 4% errors
        'anomalous'       -- 1% anomalies
    ], multi_if(
        (rand() % 100) <= 85, 1,
        (rand() % 100) <= 95, 2,
        (rand() % 100) <= 99, 3,
        4
    )),
    
    -- Comprehensive message ID list from all categories
    message_id string DEFAULT array_element([
        -- Informational messages (302xxx - Connection tracking)
        '302013', '302014', '302015', '302016', '302020', '302021',
        '302003', '302033', '302012',
        -- Informational (305xxx - State tracking)
        '305011', '305012',
        -- Informational (109xxx - Authentication)
        '109001', '109005', '109007',
        -- Informational (101xxx-105xxx - Failover/HA)
        '101001', '101002', '103002', '104004', '104500', '104502',
        '105003', '105004',
        -- Informational (212xxx - SNMP)
        '212003', '212004',
        -- Informational (303xxx - FTP)
        '303002',
        -- Informational (304xxx - URL)
        '304004',
        -- Informational (314xxx - RTSP)
        '314004',
        -- Informational (400xxx - IPS)
        '400038', '400043', '400044', '400048',
        -- Informational (502xxx - Group policy)
        '502111',
        -- Informational (718xxx - Keepalive/Hello)
        '718012', '718015', '718019', '718021', '718023',
        -- Informational (710xxx - TCP access)
        '710002', '710003',
        -- Informational (318xxx - OSPF)
        '318107',
        
        -- Warning messages (106xxx - Deny/Access control)
        '106001', '106015', '106023', '106100',
        -- Warning (313xxx - ICMP)
        '313001', '313004', '313005', '313008', '313009',
        -- Warning (304xxx - URL timeout)
        '304003',
        -- Warning (733xxx - Threat detection)
        '733102', '733104', '733105',
        -- Warning (750xxx - DoS protection)
        '750004',
        
        -- Error messages (106xxx - Spoofing/Limits)
        '106022', '106101',
        -- Error (107xxx - RIP auth)
        '107001',
        -- Error (108xxx - SMTP threats)
        '108003',
        -- Error (202xxx - NAT exhaustion)
        '202010',
        -- Error (419xxx - VPN)
        '419002',
        -- Error (430xxx - VPN)
        '430002',
        
        -- NAT (602xxx, 702xxx)
        '602303', '602304', '702307'
    ], (rand() % 64) + 1),
    
    -- Severity level (auto-determined from message ID)
    severity int8 DEFAULT multi_if(
        -- Severity 1 - Alert
        message_id IN ('101001', '101002', '103002', '104004', '104500', '104502', '105003', '105004', '106022', '106101', '107001'), 1,
        -- Severity 2 - Critical
        message_id IN ('106001', '108003'), 2,
        -- Severity 3 - Error  
        message_id IN ('212003', '212004', '304003', '313004', '313005', '318107', '202010'), 3,
        -- Severity 4 - Warning
        message_id IN ('106023', '106015', '400038', '400043', '400044', '400048', '733102', '733104', '733105'), 4,
        -- Severity 5 - Notification
        message_id IN ('502111', '718012', '718015', '750004'), 5,
        -- Severity 6 - Informational
        message_id IN ('109001', '109005', '109007', '302003', '302012', '302013', '302014', '302015', '302016', '302020', '302021', '302033', '304004', '305011', '305012', '313001', '313008', '313009', '314004', '602303', '602304', '702307', '419002', '430002'), 6,
        -- Severity 7 - Debug
        7
    ),
    
    -- Source IP addresses (realistic distribution)
    src_ip string DEFAULT multi_if(
        (rand() % 100) <= 60, concat('10.', to_string((rand() % 256)), '.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        (rand() % 100) <= 80, concat('192.168.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        (rand() % 100) <= 90, concat('172.', to_string((rand() % 16) + 16), '.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        concat(to_string((rand() % 223) + 1), '.', to_string((rand() % 256)), '.', to_string((rand() % 256)), '.', to_string((rand() % 256)))
    ),
    
    -- Destination IP addresses
    dst_ip string DEFAULT multi_if(
        (rand() % 100) <= 40, concat('10.', to_string((rand() % 256)), '.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        (rand() % 100) <= 55, concat('192.168.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        (rand() % 100) <= 65, concat('172.', to_string((rand() % 16) + 16), '.', to_string((rand() % 256)), '.', to_string((rand() % 256))),
        concat(to_string((rand() % 223) + 1), '.', to_string((rand() % 256)), '.', to_string((rand() % 256)), '.', to_string((rand() % 256)))
    ),
    
    -- Source port
    src_port uint16 DEFAULT multi_if(
        (rand() % 100) <= 70, (rand() % 30000) + 32768,  -- Ephemeral ports
        (rand() % 65535) + 1
    ),
    
    -- Destination port (weighted towards services)
    dst_port uint16 DEFAULT multi_if(
        (rand() % 100) <= 30, 443,   -- HTTPS
        (rand() % 100) <= 50, 80,    -- HTTP
        (rand() % 100) <= 65, 22,    -- SSH
        (rand() % 100) <= 75, 3389,  -- RDP
        (rand() % 100) <= 85, 53,    -- DNS
        (rand() % 100) <= 90, 21,    -- FTP
        (rand() % 100) <= 93, 25,    -- SMTP
        (rand() % 100) <= 95, 3306,  -- MySQL
        (rand() % 100) <= 97, 5432,  -- PostgreSQL
        (rand() % 65535) + 1
    ),
    
    -- Protocol
    protocol string DEFAULT array_element(['TCP', 'UDP', 'ICMP', 'ESP', 'AH', 'GRE'], multi_if(
        (rand() % 100) <= 70, 1,  -- 70% TCP
        (rand() % 100) <= 90, 2,  -- 20% UDP
        (rand() % 100) <= 97, 3,  -- 7% ICMP
        (rand() % 3) + 4          -- 3% other
    )),
    
    -- Interface names
    src_interface string DEFAULT array_element(['outside', 'inside', 'dmz', 'management', 'wan', 'lan'], (rand() % 6) + 1),
    dst_interface string DEFAULT array_element(['outside', 'inside', 'dmz', 'management', 'wan', 'lan'], (rand() % 6) + 1),
    
    -- Connection ID (for session tracking)
    connection_id uint32 DEFAULT rand(),
    
    -- Bytes transferred (realistic distribution)
    bytes_sent uint32 DEFAULT multi_if(
        protocol = 'ICMP', rand() % 1000,
        protocol = 'UDP', rand() % 50000,
        message_id IN ('302020', '302021'), rand() % 1000,  -- ICMP sessions
        rand() % 5000000  -- TCP can be large
    ),
    
    -- Username (authentication logs)
    username string DEFAULT concat(
        array_element(['admin', 'user', 'root', 'operator', 'guest', 'service', 'john', 'jane', 'bob', 'alice', 'system', 'test', 'vpnuser', 'webadmin'], (rand() % 14) + 1),
        multi_if((rand() % 100) <= 60, '', to_string((rand() % 100)))
    ),
    
    -- Action (permit/deny)
    action string DEFAULT multi_if(
        message_id IN ('106001', '106023', '106100', '313001', '313004', '313005'), 'deny',
        message_id IN ('710002', '710003', '109007'), 'permit',
        multi_if((rand() % 100) <= 85, 'permit', 'deny')
    ),
    
    -- Reason for connection teardown/denial
    reason string DEFAULT array_element([
        'No matching connection',
        'Access denied by ACL',
        'Timeout',
        'Invalid packet',
        'Port unreachable',
        'Security policy violation',
        'Connection limit reached',
        'No matching session',
        'TCP FINs',
        'TCP Reset',
        'Idle timeout',
        'SYN Timeout'
    ], (rand() % 12) + 1),
    
    -- NAT addresses
    nat_src_ip string DEFAULT concat(
        to_string((rand() % 223) + 1), '.', 
        to_string((rand() % 256)), '.', 
        to_string((rand() % 256)), '.', 
        to_string((rand() % 256))
    ),
    
    nat_dst_ip string DEFAULT concat(
        to_string((rand() % 223) + 1), '.', 
        to_string((rand() % 256)), '.', 
        to_string((rand() % 256)), '.', 
        to_string((rand() % 256))
    ),
    
    -- Session duration (in seconds)
    duration uint32 DEFAULT multi_if(
        (rand() % 100) <= 30, rand() % 60,       -- 30% very short (< 1 min)
        (rand() % 100) <= 70, rand() % 600,      -- 40% short (< 10 min)
        (rand() % 100) <= 90, rand() % 3600,     -- 20% medium (< 1 hour)
        rand() % 86400                           -- 10% long (< 1 day)
    ),
    
    -- ICMP type and code
    icmp_type uint8 DEFAULT rand() % 20,
    icmp_code uint8 DEFAULT rand() % 16,
    
    -- URL (for web-related logs)
    url string DEFAULT concat(
        'http://',
        array_element(['example', 'test', 'demo', 'sample', 'webserver', 'api', 'cdn', 'media'], (rand() % 8) + 1),
        '.',
        array_element(['com', 'net', 'org', 'io'], (rand() % 4) + 1),
        '/',
        array_element(['index.html', 'api/v1/data', 'login.php', 'admin/dashboard', 'files/download', 'images/banner.jpg', 'video/stream'], (rand() % 7) + 1)
    ),
    
    -- Access group name
    acl_name string DEFAULT array_element([
        'outside_access_in',
        'inside_access_out',
        'dmz_access',
        'management_access',
        'vpn_access',
        'guest_access',
        'default_access'
    ], (rand() % 7) + 1),
    
    -- TCP flags
    tcp_flags string DEFAULT array_element(['SYN', 'ACK', 'FIN', 'RST', 'PSH', 'URG', 'SYN ACK', 'FIN ACK'], (rand() % 8) + 1),
    
    -- Failover reason
    failover_reason string DEFAULT array_element([
        'No Active unit found',
        'Other firewall reporting failure',
        'Sequence number mismatch',
        'Configuration mismatch',
        'Interface failure',
        'Command'
    ], (rand() % 6) + 1),
    
    -- Rate limiting info (for threat detection)
    burst_rate uint16 DEFAULT rand() % 1000,
    average_rate uint16 DEFAULT rand() % 10000,
    cumulative_count uint32 DEFAULT rand() % 100000000,
    
    -- File name (for FTP logs)
    filename string DEFAULT concat(
        array_element(['report', 'data', 'backup', 'config', 'document', 'image'], (rand() % 6) + 1),
        '_',
        to_string(rand() % 1000),
        array_element(['.txt', '.pdf', '.zip', '.conf', '.log', '.jpg'], (rand() % 6) + 1)
    ),
    
    -- Error code (for various error scenarios)
    error_code string DEFAULT array_element([
        'Unavailable',
        'Not responding',
        'Timeout',
        'Connection refused',
        'Authentication failed',
        'Invalid credentials'
    ], (rand() % 6) + 1),
    
    -- Anomaly label (1 = anomaly, 0 = normal)
    is_anomaly int8 DEFAULT multi_if(
        message_category = 'anomalous', 1,
        message_category = 'error', multi_if((rand() % 100) <= 80, 1, 0),
        message_category = 'warning', multi_if((rand() % 100) <= 20, 1, 0),
        0
    ),
    
    -- Full syslog message (composite field)
    log_message string DEFAULT concat(
        format_datetime(timestamp, '%b %d %Y %H:%M:%S'), ' ',
        device_name, ': %ASA-', to_string(severity), '-', message_id, ': ',
        multi_if(
            -- ========== CONNECTION MESSAGES (302xxx) ==========
            message_id = '302013', concat(
                'Built ', lower(protocol), ' connection ', to_string(connection_id), 
                ' for ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' (', nat_src_ip, '/', to_string(src_port), ') to ', 
                dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' (', nat_dst_ip, '/', to_string(dst_port), ')'
            ),
            
            message_id = '302015', concat(
                'Built ', lower(protocol), ' connection for faddr ', dst_ip, '/', to_string(dst_port), 
                ' gaddr ', nat_dst_ip, '/', to_string(dst_port), 
                ' laddr ', src_ip, '/', to_string(src_port)
            ),
            
            message_id = '302014', concat(
                'Teardown ', protocol, ' connection ', to_string(connection_id), 
                ' for ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' duration ', to_string(duration), ' seconds bytes ', to_string(bytes_sent), 
                ' ', reason
            ),
            
            message_id = '302016', concat(
                'Teardown ', protocol, ' connection ', to_string(connection_id), 
                ' for ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' duration ', to_string(duration), ' seconds bytes ', to_string(bytes_sent)
            ),
            
            message_id = '302020', concat(
                'Built inbound ICMP connection for faddr ', dst_ip, '/', to_string(icmp_type), 
                ' gaddr ', nat_dst_ip, '/', to_string(icmp_type), ' laddr ', src_ip, '/', to_string(icmp_type)
            ),
            
            message_id = '302021', concat(
                'Teardown ICMP connection for faddr ', dst_ip, '/', to_string(icmp_type), 
                ' gaddr ', nat_dst_ip, '/', to_string(icmp_type), ' laddr ', src_ip, '/', to_string(icmp_type)
            ),
            
            message_id = '302003', concat(
                'Built H245 connection for foreign_address ', dst_ip, '/', to_string(dst_port), 
                ' local_address ', src_ip, '/', to_string(src_port)
            ),
            
            message_id = '302033', concat(
                'Pre-allocated H323 GUP Connection for faddr ', src_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' to laddr ', dst_interface, ':', src_ip, '/', to_string(src_port)
            ),
            
            message_id = '302012', concat(
                'Teardown ', protocol, ' connection ', to_string(connection_id), 
                ' for ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' duration ', to_string(duration), ' seconds bytes ', to_string(bytes_sent), ' ', reason
            ),
            
            -- ========== DENIAL/ACCESS CONTROL (106xxx) ==========
            message_id = '106023', concat(
                'Deny ', protocol, ' src ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' dst ', dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ' by access-group "', acl_name, '"'
            ),
            
            message_id = '106001', concat(
                'Inbound ', protocol, ' connection denied from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_ip, '/', to_string(dst_port), ' flags ', tcp_flags, 
                ' on interface ', src_interface
            ),
            
            message_id = '106100', concat(
                'access-list ', acl_name, ' ', action, 'd ', protocol, ' ', 
                src_interface, '/', src_ip, '(', to_string(src_port), ') -> ', 
                dst_interface, '/', dst_ip, '(', to_string(dst_port), ') hit-cnt ', 
                to_string(rand() % 10000), ' ', 
                array_element(['first hit', '300-second interval'], (rand() % 2) + 1)
            ),
            
            message_id = '106015', concat(
                'Deny ', protocol, ' (no connection) from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_ip, '/', to_string(dst_port), ' flags ', tcp_flags, 
                ' on interface ', src_interface
            ),
            
            message_id = '106022', concat(
                'Deny protocol connection spoof from ', src_ip, ' to ', dst_ip, 
                ' on interface ', src_interface
            ),
            
            message_id = '106101', 'Number of cached deny-flows for ACL log has reached limit (100000).',
            
            -- ========== ICMP MESSAGES (313xxx) ==========
            message_id = '313001', concat(
                'Denied ICMP type=', to_string(icmp_type), ', code=', to_string(icmp_code), 
                ' from ', src_ip, ' on interface ', src_interface
            ),
            
            message_id = '313004', concat(
                'Denied ICMP type=', to_string(icmp_type), 
                ', from ', src_ip, ' on interface ', src_interface, ' to ', dst_ip, 
                ': no matching session'
            ),
            
            message_id = '313005', concat(
                'No matching connection for ICMP error message: icmp src ', 
                src_interface, ':', src_ip, ' dst ', dst_interface, ':', dst_ip, 
                ' (type ', to_string(icmp_type), ', code ', to_string(icmp_code), ')'
            ),
            
            message_id = '313008', concat(
                'Denied ICMP type=', to_string(icmp_type), ', code=', to_string(icmp_code), 
                ' from ', src_ip, ' on interface ', src_interface, ' to ', dst_ip, 
                ': no matching connection'
            ),
            
            message_id = '313009', concat(
                'Denied invalid ICMP code ', to_string(icmp_code), ', for type=', to_string(icmp_type), 
                ' from ', src_ip, ' on interface ', src_interface
            ),
            
            -- ========== STATE MESSAGES (305xxx) ==========
            message_id = '305011', concat(
                'Built ', lower(protocol), ' state for ', src_interface, ' address ', src_ip, 
                ' port ', to_string(src_port), ' (', nat_src_ip, ':', to_string(src_port), ')'
            ),
            
            message_id = '305012', concat(
                'Teardown ', lower(protocol), ' state for ', src_interface, ' address ', src_ip, 
                ' port ', to_string(src_port), ' (', nat_src_ip, ':', to_string(src_port), ')'
            ),
            
            -- ========== AUTHENTICATION (109xxx) ==========
            message_id = '109001', concat(
                'Auth start for user ', username, ' from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_ip, '/', to_string(dst_port)
            ),
            
            message_id = '109005', concat(
                'Authentication succeeded for user ', username, ' from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_ip, '/', to_string(dst_port), ' on interface ', src_interface
            ),
            
            message_id = '109007', concat(
                'Authorization permitted for user ', username, ' from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_ip, '/', to_string(dst_port), ' on interface ', src_interface
            ),
            
            -- ========== FAILOVER MESSAGES (101xxx-105xxx) ==========
            message_id = '101001', '(Primary) Failover cable OK.',
            message_id = '101002', '(Primary) Bad failover cable.',
            message_id = '103002', concat('(Primary) Other firewall network interface ', src_interface, ' OK.'),
            message_id = '104004', '(Primary) Switching to OK.',
            message_id = '104500', concat('(Primary) Switching to ACTIVE (cause: ', failover_reason, ')'),
            message_id = '104502', '(Primary) Becoming Backup unit failed.',
            message_id = '105003', concat('(Primary) Monitoring on interface ', src_interface, ' waiting'),
            message_id = '105004', concat('(Primary) Monitoring on interface ', src_interface, ' normal'),
            
            -- ========== SNMP (212xxx) ==========
            message_id = '212003', concat(
                'Unable to receive an SNMP request on interface ', src_interface, 
                ', error code = ', error_code, ', will try again'
            ),
            
            message_id = '212004', concat(
                'Unable to send an SNMP response to IP Address ', src_ip, 
                ' Port ', to_string(src_port), ' interface ', src_interface, 
                ', error code = ', error_code
            ),
            
            -- ========== FTP (303xxx) ==========
            message_id = '303002', concat(
                'FTP connection from ', src_interface, ':', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port), 
                ', user ', username, ' action file ', filename
            ),
            
            -- ========== URL/WEB (304xxx, 314xxx) ==========
            message_id = '304003', concat('URL Server ', src_ip, ' timed out URL ', url),
            message_id = '304004', concat('URL Server ', src_ip, ' request failed URL ', url),
            message_id = '314004', concat('RTSP client ', src_interface, ':', src_ip, ' accessed RTSP URL ', url),
            
            -- ========== IPS (400xxx) ==========
            message_id = '400038', concat(
                'IPS:6100 RPC Port Registration ', src_ip, ' to ', dst_ip, 
                ' on interface ', src_interface
            ),
            message_id = '400043', concat(
                'IPS:6151 ypbind (YP bind daemon) Portmap Request ', src_ip, ' to ', dst_ip, 
                ' on interface ', src_interface
            ),
            message_id = '400044', concat(
                'IPS:6152 yppasswdd (YP password daemon) Portmap Request ', src_ip, ' to ', dst_ip, 
                ' on interface ', src_interface
            ),
            message_id = '400048', concat(
                'IPS:6175 rexd (remote execution daemon) Portmap Request ', src_ip, ' to ', dst_ip, 
                ' on interface ', src_interface
            ),
            
            -- ========== GROUP POLICY (502xxx) ==========
            message_id = '502111', concat('New group policy added: name: ', acl_name, ' Type: external'),
            
            -- ========== TCP ACCESS (710xxx) ==========
            message_id = '710002', concat(
                'TCP access permitted from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port)
            ),
            
            message_id = '710003', concat(
                'TCP access denied by ACL from ', src_ip, '/', to_string(src_port), 
                ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port)
            ),
            
            -- ========== KEEPALIVE/HELLO (718xxx) ==========
            message_id = '718012', concat('Sent HELLO request to ', src_ip),
            message_id = '718015', concat('Received HELLO request from ', src_ip),
            message_id = '718019', concat('Sent KEEPALIVE request to ', src_ip),
            message_id = '718021', concat('Sent KEEPALIVE response to ', src_ip),
            message_id = '718023', concat('Received KEEPALIVE response from ', src_ip),
            
            -- ========== OSPF (318xxx) ==========
            message_id = '318107', concat('OSPF is enabled on ', src_interface, ' during configuration'),
            
            -- ========== THREAT DETECTION (733xxx) ==========
            message_id = '733102', concat('Threat-detection adds host ', src_ip, ' to shun list'),
            message_id = '733104', 'TD_SYSLOG_TCP_INTERCEPT_AVERAGE_RATE_EXCEED',
            message_id = '733105', 'TD_SYSLOG_TCP_INTERCEPT_BURST_RATE_EXCEED',
            
            -- ========== DOS PROTECTION (750xxx) ==========
            message_id = '750004', concat(
                'Local: ', src_ip, ':', to_string(src_port), 
                ' Remote: ', dst_ip, ':', to_string(dst_port), 
                ' Username: ', username, ' Sending COOKIE challenge to throttle possible DoS'
            ),
            
            -- ========== NAT (602xxx, 702xxx) ==========
            message_id = '602303', concat(
                'NAT: ', src_ip, '/', to_string(src_port), ' to ', 
                nat_src_ip, '/', to_string(src_port)
            ),
            message_id = '602304', concat(
                'NAT: ', dst_ip, '/', to_string(dst_port), ' to ', 
                nat_dst_ip, '/', to_string(dst_port)
            ),
            message_id = '702307', concat(
                'Dynamic NAT pool exhausted. Unable to create connection from ', 
                src_ip, '/', to_string(src_port), ' to ', dst_ip, '/', to_string(dst_port)
            ),
            
            -- ========== ERROR MESSAGES ==========
            message_id = '107001', concat(
                'RIP auth failed from ', src_ip, ': version=2, type=string, mode=string, sequence=', 
                to_string(rand() % 1000), ' on interface ', src_interface
            ),
            
            message_id = '108003', concat(
                'Terminating SMTP connection; malicious pattern detected in the mail address from ', 
                src_interface, ':', src_ip, '/', to_string(src_port)
            ),
            
            message_id = '202010', concat(
                'PAT pool exhausted. Unable to create ', protocol, ' connection from ', 
                src_ip, '/', to_string(src_port), ' to ', dst_ip, '/', to_string(dst_port)
            ),
            
            message_id = '419002', concat('VPN error: ', error_code),
            message_id = '430002', concat('VPN connection error from ', src_ip),
            
            -- Default fallback
            concat('Event for message ID ', message_id, ' from ', src_ip, ' to ', dst_ip)
        )
    )
) SETTINGS eps = 100;


CREATE EXTERNAL STREAM IF NOT EXISTS cisco.asa_logs_stream (
    message string
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'cisco_asa_logs', data_format='JSONEachRow', one_message_per_row=true;


CREATE MATERIALIZED VIEW IF NOT EXISTS cisco.mv_asa_logs
INTO cisco.asa_logs_stream
AS
SELECT
    log_message AS message
FROM cisco.cisco_asa_logs;