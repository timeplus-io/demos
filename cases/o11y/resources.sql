CREATE DATABASE IF NOT EXISTS o11y;

-- source from external streams

CREATE STREAM o11y.opensearch_t1
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'http', 
    data_format = 'OpenSearch', 
    url = 'http://34.187.249.72:9200/otlp_metrics/_bulk', 
    skip_ssl_cert_check = true
COMMENT 'send message to opensearch.demo.timeplus.com';

CREATE STREAM o11y.otlp_metrics
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'otlp_metrics';


CREATE STREAM o11y.splunk_t1
(
  `event` string,
  sourcetype string default 'otel'
)
ENGINE = ExternalStream
SETTINGS type = 'http', http_header_Authorization = 'Splunk f50aef7d-bd49-4ff3-90f9-d8ac54ecbe37', url = 'http://35.230.87.146:8088/services/collector'
COMMENT 'send message to splunk.demo.timeplus.com';

-- MV
CREATE MATERIALIZED VIEW o11y.mv_otel_kafka2opensearch INTO o11y.opensearch_t1
AS
SELECT
  raw
FROM
  o11y.otlp_metrics
COMMENT 'Read OpenTelemetry JSON messages from Kafka, apply optional filter/transformation and write to OpenSearch index';


CREATE MATERIALIZED VIEW o11y.mv_otel_kafka2splunk INTO o11y.splunk_t1
(
  `event` string,
) AS(
SELECT
  raw as event
FROM
  o11y.otlp_metrics)
COMMENT 'Read OpenTelemetry JSON messages from Kafka, apply optional filter/transformation and write to Splunk index';
