CREATE RANDOM STREAM cisco_asa_simulator.cisco_asa_tcp_connections (
    join_key int8 DEFAULT 1,
    connection_id int64 DEFAULT 1 + (rand(1) % 9999999),
    event_time datetime64(3) DEFAULT now64(3),
    direction string DEFAULT multi_if((rand(2) % 100) <= 60, 'inbound', 'outbound'),
    src_interface string DEFAULT array_element(['outside', 'dmz', 'partner', 'inside', 'trusted'], (rand(3) % 5) + 1),
    dst_interface string DEFAULT array_element(['inside', 'dmz', 'outside', 'internet'], (rand(4) % 4) + 1),
    src_ip string DEFAULT concat(to_string(1 + (rand(5) % 223)), '.', to_string(rand(6) % 256), '.', to_string(rand(7) % 256), '.', to_string(1 + (rand(8) % 254))),
    src_port int32 DEFAULT 1024 + (rand(9) % 64512),
    dst_ip string DEFAULT concat(to_string(1 + (rand(10) % 223)), '.', to_string(rand(11) % 256), '.', to_string(rand(12) % 256), '.', to_string(1 + (rand(13) % 254))),
    dst_port int32 DEFAULT array_element([80, 443, 8080, 8443, 3306, 5432, 6379, 27017, 9200, 22, 3389], (rand(14) % 11) + 1),
    
    -- NAT IP addresses (50% chance of being different from original)
    nat_src_ip string DEFAULT multi_if(
        (rand(25) % 100) <= 50, src_ip,
        concat(to_string(1 + (rand(26) % 223)), '.', to_string(rand(27) % 256), '.', to_string(rand(28) % 256), '.', to_string(1 + (rand(29) % 254)))
    ),
    nat_src_port int32 DEFAULT multi_if(
        (rand(30) % 100) <= 50, src_port,
        1024 + (rand(31) % 64512)
    ),
    nat_dst_ip string DEFAULT multi_if(
        (rand(32) % 100) <= 50, dst_ip,
        concat(to_string(1 + (rand(33) % 223)), '.', to_string(rand(34) % 256), '.', to_string(rand(35) % 256), '.', to_string(1 + (rand(36) % 254)))
    ),
    nat_dst_port int32 DEFAULT multi_if(
        (rand(37) % 100) <= 50, dst_port,
        1024 + (rand(38) % 64512)
    ),
    
    duration_ms int32 DEFAULT multi_if((rand(15) % 100) <= 40, 100 + (rand(16) % 900), (rand(17) % 100) <= 75, 1000 + (rand(18) % 4000), (rand(19) % 100) <= 92, 5000 + (rand(20) % 10000), 15000 + (rand(21) % 15000)),
    bytes_sent int32 DEFAULT 1024 + (rand(22) % 51200),
    bytes_received int32 DEFAULT 1024 + (rand(23) % 102400),
    teardown_reason string DEFAULT array_element(['TCP FINs', 'TCP Reset-I', 'TCP Reset-O', 'Idle timeout', 'Denied by ACL', 'SYN Timeout'], (rand(39) % 6) + 1),
    end_time datetime64(3) DEFAULT date_add(millisecond, duration_ms, event_time),
    
    -- Duration in HH:MM:SS format
    duration_str string DEFAULT concat(
        lpad(to_string(floor(duration_ms / 3600000)), 2, '0'), ':',
        lpad(to_string(floor((duration_ms % 3600000) / 60000)), 2, '0'), ':',
        lpad(to_string(floor((duration_ms % 60000) / 1000)), 2, '0')
    ),
    
    -- Built message with NAT addresses in parentheses
    built_message string DEFAULT concat(
        '%ASA-6-302013: Built ', direction, ' TCP connection ', to_string(connection_id), 
        ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
        ' (', nat_src_ip, '/', to_string(nat_src_port), ')',
        ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
        ' (', nat_dst_ip, '/', to_string(nat_dst_port), ')'
    ),
    
    -- Teardown message with proper duration format
    teardown_message string DEFAULT concat(
        '%ASA-6-302014: Teardown TCP connection ', to_string(connection_id),
        ' for ', src_interface, ':', src_ip, '/', to_string(src_port),
        ' to ', dst_interface, ':', dst_ip, '/', to_string(dst_port),
        ' duration ', duration_str, ' bytes ', to_string(bytes_sent + bytes_received),
        ' ', teardown_reason
    ),

    -- Device identifier (asa-fw01 to asa-fw25)
    device_name string DEFAULT concat('asa-fw', lpad(to_string((rand(24) % 26) + 1), 2, '0')),

    -- Complete built log (with syslog header)
    built_log string DEFAULT concat('<190>', format_datetime(event_time, '%b %e %H:%M:%S'), ' ', device_name, ' ', built_message),
    
    -- Complete teardown log (with syslog header)
    teardown_log string DEFAULT concat('<190>', format_datetime(end_time, '%b %e %H:%M:%S'), ' ', device_name, ' ', teardown_message)
) SETTINGS eps = 100, interval_time = 10;

-- UDF to emit session end message based on end_time
CREATE OR REPLACE AGGREGATE FUNCTION time_based_emit(end_time datetime64, message string)
RETURNS string
LANGUAGE JAVASCRIPT AS $$
{
    has_customized_emit: true,
    
    initialize: function() {
        this.queue = [];  // Internal queue to store all data
        this.result = []; // Result array for data to emit
    },
    
    process: function(end_times, messages) {
        // Add incoming data to the internal queue
        for (let i = 0; i < end_times.length; i++) {
            this.queue.push({
                end_time: end_times[i],
                message: messages[i]
            });
        }
        
        // Get current time in milliseconds
        let current_time = Date.now();
        
        // Filter and emit data where end_time > current_time
        let remaining_queue = [];
        for (let i = 0; i < this.queue.length; i++) {
            if (this.queue[i].end_time <= current_time) {
                // Emit this record with debug info
                let debug_message = this.queue[i].message + 
                    ' | end_time: ' + new Date(this.queue[i].end_time).toISOString() + 
                    ' | current_time: ' + new Date(current_time).toISOString();
                // this.result.push(debug_message);
                this.result.push(this.queue[i].message);

            } else {
                // Keep in queue
                remaining_queue.push(this.queue[i]);
            }
        }
        
        // Update queue with remaining items (remove emitted data)
        this.queue = remaining_queue;
        
        // Return number of emitted items (>0 will trigger finalize)
        return this.result.length;
    },
    
    finalize: function() {
        // Return the result and reset for next aggregation
        let old_result = this.result;
        this.result = [];
        return old_result;
    },
    
    serialize: function() {
        return JSON.stringify({
            'queue': this.queue,
            'result': this.result
        });
    },
    
    deserialize: function(state_str) {
        let s = JSON.parse(state_str);
        this.queue = s['queue'] || [];
        this.result = s['result'] || [];
    },
    
    merge: function(state_str) {
        let s = JSON.parse(state_str);
        // Merge queues from different partitions
        if (s['queue']) {
            this.queue = this.queue.concat(s['queue']);
        }
        if (s['result']) {
            this.result = this.result.concat(s['result']);
        }
    }
}
$$;

CREATE STREAM cisco_asa_simulator.cisco_asa_session_logs
(
    event_time datetime64(3),
    end_time datetime64(3),
    built_log string,
    teardown_log string
) 
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


CREATE MATERIALIZED VIEW IF NOT EXISTS cisco_asa_simulator.mv_session_logs
INTO cisco_asa_simulator.cisco_asa_session_logs
AS
SELECT
    event_time, end_time, built_log, teardown_log
FROM cisco_asa_simulator.cisco_asa_tcp_connections;

CREATE STREAM cisco_asa_simulator.cisco_asa_splited_session_logs
(
    message string
) 
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


-- MV send session logs to kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS cisco_asa_simulator.mv_tcp_session_start_logs
INTO cisco_asa_simulator.cisco_asa_splited_session_logs
AS
SELECT
    built_log AS message
FROM cisco_asa_simulator.cisco_asa_session_logs;

CREATE MATERIALIZED VIEW IF NOT EXISTS cisco_asa_simulator.mv_tcp_session_end_logs
INTO cisco_asa_simulator.cisco_asa_splited_session_logs
AS
SELECT
  time_based_emit(end_time, teardown_log) AS message
FROM
  cisco_asa_simulator.cisco_asa_session_logs




