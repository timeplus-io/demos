CREATE STREAM IF NOT EXISTS invest_insights.stock_ext (
    `raw` string
)
ENGINE = ExternalStream
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_stock',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'RawBLOB',
    one_message_per_row = true;

CREATE STREAM IF NOT EXISTS invest_insights.stock_w_ext (
    `event_ts` uint64,
    `SecurityID` string,
    `Symbol` string,
    `PreClosePx` float64,
    `LastPx` float64,
    `OpenPx` float64,
    `ClosePx` float64,
    `HighPx` float64,
    `LowPx` float64
)
ENGINE = ExternalStream
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_stock',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'JSONEachRow',
    one_message_per_row = true;

CREATE MUTABLE STREAM IF NOT EXISTS invest_insights.stock (
    `event_ts` uint64,
    `SecurityID` string,
    `Symbol` string,
    `PreClosePx` float64,
    `LastPx` float64,
    `OpenPx` float64,
    `ClosePx` float64,
    `HighPx` float64,
    `LowPx` float64
)
PRIMARY KEY SecurityID
SETTINGS logstore_retention_bytes = 107374182, logstore_retention_ms = 300000;
