-- simulate an attack
CREATE DATABASE IF NOT EXISTS cisco_asa_simulator;


-- normal traffic eps = 5
CREATE RANDOM STREAM cisco_asa_simulator.cisco_asa_ddos_normal (
    -- Timestamp
    timestamp datetime64(3) DEFAULT now64(3),

    -- Fixed attacker device and IP
    device_name string DEFAULT 'asa-fw01',
    src_ip string DEFAULT '203.0.113.66',

    -- Random destination IPs (scanning many targets)
    dst_ip string DEFAULT concat(
        '10.',
        to_string((rand(1) % 256)), '.',
        to_string((rand(2) % 256)), '.',
        to_string((rand(3) % 256))
    ),

    -- Alternate between 302014 (TCP teardown) and 302016 (UDP teardown)
    message_id string DEFAULT multi_if(
        (rand(4) % 100) <= 60, '302014',
        '302016'
    ),

    -- Severity 6 for both message IDs
    severity int8 DEFAULT 6,

    -- Ports
    src_port uint16 DEFAULT (rand(5) % 30000) + 32768,
    dst_port uint16 DEFAULT multi_if(
        (rand(6) % 100) <= 30, 443,
        (rand(7) % 100) <= 50, 80,
        (rand(8) % 100) <= 65, 22,
        (rand(9) % 100) <= 75, 53,
        (rand(10) % 65535) + 1
    ),

    -- Protocol matches message ID: 302014 = TCP, 302016 = UDP
    protocol string DEFAULT multi_if(message_id = '302014', 'TCP', 'UDP'),

    -- Interfaces
    src_interface string DEFAULT 'outside',
    dst_interface string DEFAULT 'inside',

    -- Connection tracking
    connection_id uint32 DEFAULT rand(11),

    -- High bytes to spike the ratio
    bytes_sent uint32 DEFAULT (rand(12) % 5000000) + 500000,

    -- Short durations (rapid fire connections)
    duration_seconds uint16 DEFAULT (rand(13) % 10) + 1,
    duration string DEFAULT concat(
        '00:00:',
        lpad(to_string(duration_seconds), 2, '0')
    ),

    -- TCP flags (only for 302014)
    tcp_flags string DEFAULT array_element(
        ['TCP FINs', 'TCP RSTs', 'TCP SYNs', 'TCP data'],
        (rand(14) % 4) + 1
    ),

    -- Direction
    direction string DEFAULT 'Inbound',

    -- Priority = 184 + severity
    priority uint8 DEFAULT 190,

    -- Message text
    message_text string DEFAULT multi_if(
        message_id = '302014', concat(
            'Teardown TCP connection ', to_string(connection_id),
            ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
            ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
            ' duration ', duration,
            ' bytes ', to_string(bytes_sent), ' ', tcp_flags
        ),
        concat(
            'Teardown UDP connection ', to_string(connection_id),
            ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
            ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
            ' duration ', duration,
            ' bytes ', to_string(bytes_sent)
        )
    ),

    -- Final syslog format
    log_message string DEFAULT concat(
        '<', to_string(priority), '>',
        format_datetime(timestamp, '%b %e %H:%M:%S'),
        ' ', device_name,
        ' %ASA-', to_string(severity), '-', message_id, ': ',
        message_text
    )
) SETTINGS eps = 5;

-- attack traffic eps = 500

CREATE RANDOM STREAM cisco_asa_simulator.cisco_asa_ddos_attack (
    -- Timestamp
    timestamp datetime64(3) DEFAULT now64(3),

    -- Fixed attacker device and IP
    device_name string DEFAULT 'asa-fw01',
    src_ip string DEFAULT '203.0.113.66',

    -- Random destination IPs (scanning many targets)
    dst_ip string DEFAULT concat(
        '10.',
        to_string((rand(1) % 256)), '.',
        to_string((rand(2) % 256)), '.',
        to_string((rand(3) % 256))
    ),

    -- Alternate between 302014 (TCP teardown) and 302016 (UDP teardown)
    message_id string DEFAULT multi_if(
        (rand(4) % 100) <= 60, '302014',
        '302016'
    ),

    -- Severity 6 for both message IDs
    severity int8 DEFAULT 6,

    -- Ports
    src_port uint16 DEFAULT (rand(5) % 30000) + 32768,
    dst_port uint16 DEFAULT multi_if(
        (rand(6) % 100) <= 30, 443,
        (rand(7) % 100) <= 50, 80,
        (rand(8) % 100) <= 65, 22,
        (rand(9) % 100) <= 75, 53,
        (rand(10) % 65535) + 1
    ),

    -- Protocol matches message ID: 302014 = TCP, 302016 = UDP
    protocol string DEFAULT multi_if(message_id = '302014', 'TCP', 'UDP'),

    -- Interfaces
    src_interface string DEFAULT 'outside',
    dst_interface string DEFAULT 'inside',

    -- Connection tracking
    connection_id uint32 DEFAULT rand(11),

    -- High bytes to spike the ratio
    bytes_sent uint32 DEFAULT (rand(12) % 5000000) + 500000,

    -- Short durations (rapid fire connections)
    duration_seconds uint16 DEFAULT (rand(13) % 10) + 1,
    duration string DEFAULT concat(
        '00:00:',
        lpad(to_string(duration_seconds), 2, '0')
    ),

    -- TCP flags (only for 302014)
    tcp_flags string DEFAULT array_element(
        ['TCP FINs', 'TCP RSTs', 'TCP SYNs', 'TCP data'],
        (rand(14) % 4) + 1
    ),

    -- Direction
    direction string DEFAULT 'Inbound',

    -- Priority = 184 + severity
    priority uint8 DEFAULT 190,

    -- Message text
    message_text string DEFAULT multi_if(
        message_id = '302014', concat(
            'Teardown TCP connection ', to_string(connection_id),
            ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
            ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
            ' duration ', duration,
            ' bytes ', to_string(bytes_sent), ' ', tcp_flags
        ),
        concat(
            'Teardown UDP connection ', to_string(connection_id),
            ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
            ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
            ' duration ', duration,
            ' bytes ', to_string(bytes_sent)
        )
    ),

    -- Final syslog format
    log_message string DEFAULT concat(
        '<', to_string(priority), '>',
        format_datetime(timestamp, '%b %e %H:%M:%S'),
        ' ', device_name,
        ' %ASA-', to_string(severity), '-', message_id, ': ',
        message_text
    )
) SETTINGS eps = 500;

-- target log stream
CREATE EXTERNAL STREAM IF NOT EXISTS cisco_asa_simulator.asa_logs_stream (
    message string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092', 
    topic = 'cisco_asa_logs', 
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    config_file='etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format='JSONEachRow', 
    one_message_per_row=true;

-- normal traffic
CREATE MATERIALIZED VIEW IF NOT EXISTS cisco_asa_simulator.mv_asa_logs_normal
INTO cisco_asa_simulator.asa_logs_stream
AS
SELECT
    log_message AS message
FROM cisco_asa_simulator.cisco_asa_ddos_normal;

-- attack traffic
CREATE MATERIALIZED VIEW IF NOT EXISTS cisco_asa_simulator.mv_asa_logs_attack
INTO cisco_asa_simulator.asa_logs_stream
AS
SELECT
    log_message AS message
FROM cisco_asa_simulator.cisco_asa_ddos_attack;


SELECT
  src_ip, live_bytes, overall_spike_ratio, hourly_spike_ratio
FROM
  cisco_observability_ddos.cxt_ddos_stream
WHERE
  src_ip = '203.0.113.66';


-- by default, pause the attack MV, when resume it, should see the alert
SYSTEM PAUSE MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;
SYSTEM RESUME MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;
