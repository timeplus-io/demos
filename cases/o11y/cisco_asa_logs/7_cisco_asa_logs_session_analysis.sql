
CREATE VIEW cisco_asa_simulator.v_cisco_asa_tcp_connections AS
WITH tcp_connection_logs AS (
SELECT
  _tp_time,
  message, 
  grok(message, '<%{POSINT:priority}>%{DATA:timestamp} %{HOSTNAME:device_name} %%{WORD:facility}-%{INT:severity}-%{INT:message_id}: %{GREEDYDATA:asa_message}') AS base_fields,
  multi_if(
        -- ============================================================
        -- 302013: Built TCP/UDP connection (HAS NAT IPs in parentheses)
        -- Format: Built {inbound|outbound} {TCP|UDP} connection ID for src_ifc:src_ip/src_port (nat_src_ip/nat_src_port) to dst_ifc:dst_ip/dst_port (nat_dst_ip/nat_dst_port)
        -- ============================================================
        base_fields['message_id'] =  '302013',
        grok(base_fields['asa_message'],
             'Built %{DATA:direction} %{DATA:protocol} connection %{INT:connection_id} for %{DATA:src_interface}:%{IP:src_ip}/%{INT:src_port} \\(%{IP:nat_src_ip}/%{INT:nat_src_port}\\) to %{DATA:dst_interface}:%{IP:dst_ip}/%{INT:dst_port} \\(%{IP:nat_dst_ip}/%{INT:nat_dst_port}\\)'),
        
        -- ============================================================
        -- 302014: Teardown TCP/UDP connection (NO NAT IPs, has duration/bytes/reason)
        -- Format: Teardown {TCP|UDP} connection ID for src_ifc:src_ip/src_port to dst_ifc:dst_ip/dst_port duration H:MM:SS bytes ### reason
        -- ============================================================
        base_fields['message_id'] =  '302014',
        grok(base_fields['asa_message'],
             'Teardown %{DATA:protocol} connection %{INT:connection_id} for %{DATA:src_interface}:%{IP:src_ip}/%{INT:src_port} to %{DATA:dst_interface}:%{IP:dst_ip}/%{INT:dst_port} duration %{DATA:duration} bytes %{INT:bytes} %{GREEDYDATA:reason}'),
         map_cast(['message_id'], [base_fields['message_id']])
    ) as asa_message_parsed
 
FROM
  cisco_asa_simulator.cisco_asa_splited_session_logs
)
SELECT 
    base_fields['priority'] as priority, 
    base_fields['timestamp'] as log_timestamp, 
    base_fields['device_name'] as device_name, 
    base_fields['severity'] as severity, 
    base_fields['message_id'] as message_id, 
    asa_message_parsed['direction'] as direction, 
    asa_message_parsed['protocol'] as protocol, 
    asa_message_parsed['connection_id'] as connection_id, 
    asa_message_parsed['src_interface'] as src_interface, 
    asa_message_parsed['src_ip'] as src_ip, 
    asa_message_parsed['src_port'] as src_port, 
    asa_message_parsed['nat_src_ip'] as nat_src_ip, 
    asa_message_parsed['nat_src_port'] as nat_src_port, 
    asa_message_parsed['dst_interface'] as dst_interface, 
    asa_message_parsed['dst_ip'] as dst_ip, 
    asa_message_parsed['dst_port'] as dst_port, 
    asa_message_parsed['nat_dst_ip'] as nat_dst_ip, 
    asa_message_parsed['duration'] as duration, 
    asa_message_parsed['bytes'] as bytes, 
    asa_message_parsed['reason'] as reason, 
    _tp_time
FROM tcp_connection_logs

-- analysis matching session and the session duration
WITH tcp_connection_logs AS
  (
    SELECT
      _tp_time, log_timestamp, device_name, severity, message_id, connection_id, message_id = '302013' AS session_start, message_id = '302014' AS session_end
    FROM
      cisco_asa_simulator.v_cisco_asa_tcp_connections
    WHERE
      message_id IN ('302013', '302014')
  )
SELECT
  device_name, count(*), count_if(message_id = '302013') AS start_count, count_if(message_id = '302014') AS end_count, min(_tp_time) AS session_start_ts, max(_tp_time) AS session_end_ts, date_diff('ms', session_start_ts, session_end_ts) AS time_to_successful_connect_ms
FROM
  tcp_connection_logs
GROUP BY
  device_name
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, session_end) WITH MAXSPAN 60s AND TIMEOUT 100s


-- session window
SELECT
  window_start, window_end,
  device_name, 
  connection_id, 
  src_ip,dst_ip,
  count(*) AS c, 
  latest(reason) as reasons,
  latest(duration) as duration
FROM
  session(cisco_asa_simulator.v_cisco_asa_tcp_connections, 10m, 30m, message_id = '302013', message_id = '302014')
PARTITION BY connection_id
GROUP BY
  window_start, window_end, device_name, connection_id, src_ip,dst_ip 
