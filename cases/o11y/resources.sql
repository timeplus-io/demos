CREATE DATABASE IF NOT EXISTS o11y;

-- source from external streams

CREATE STREAM o11y.opensearch_t1
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'http', 
    data_format = 'OpenSearch', 
    url = 'https://opensearch.demo.timeplus.com:9200/otlp_metrics/_bulk', 
    skip_ssl_cert_check = true, 
    username = 'admin', 
    password = 'kdjkdg_ddg2K14'
COMMENT 'send message to opensearch.demo.timeplus.com';

CREATE STREAM o11y.otlp_metrics
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'otlp_metrics';

CREATE STREAM o11y.splunk_t1
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'http', data_format = 'JSONEachRow', http_header_Authorization = 'Splunk 8367dcdd-cbad-4770-96da-8367240f11bb', url = 'http://hec.splunk.demo.timeplus.com:8088/services/collector/event'
COMMENT 'send message to splunk.demo.timeplus.com';

-- MV
CREATE MATERIALIZED VIEW o11y.mv_otel_kafka2opensearch INTO o11y.opensearch_t1
(
  `raw` string,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS(
SELECT
  raw
FROM
  o11y.otlp_metrics)
COMMENT 'Read OpenTelemetry JSON messages from Kafka, apply optional filter/transformation and write to OpenSearch index';

CREATE MATERIALIZED VIEW o11y.mv_otel_kafka2splunk INTO o11y.splunk_t1
(
  `raw` string,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS(
SELECT
  raw
FROM
  o11y.otlp_metrics)
COMMENT 'Read OpenTelemetry JSON messages from Kafka, apply optional filter/transformation and write to Splunk index';





