CREATE DATABASE IF NOT EXISTS invest_insights;

-- position
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.position_ext (
    raw string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_position',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'RawBLOB',
    one_message_per_row = true;

CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.position_w_ext (
  event_ts uint64,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  HoldingQty float64
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_position',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'JSONEachRow',
    one_message_per_row = true;


CREATE Mutable STREAM IF NOT EXISTS invest_insights.position (
  event_ts uint64,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  HoldingQty float64
)
PRIMARY KEY(SecurityAccount, SecurityId)
SETTINGS logstore_retention_bytes = 107374182, logstore_retention_ms = 300000;

-- execution
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.execution_ext (
    raw string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_execution',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'RawBLOB',
    one_message_per_row = true;


CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.execution_w_ext (
  event_ts uint64,
  OrderId string,
  TradeDate date,
  SecurityAccount string,
  SecurityId low_cardinality(string),
  EntrustDirection string,
  LastQty float64,
  LastPx float64,
  Fee float64,
  StrategyId string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_execution',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'JSONEachRow',
    one_message_per_row = true;

CREATE STREAM IF NOT EXISTS invest_insights.execution (
  event_ts uint64,
  OrderId string,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  EntrustDirection string,
  LastQty float64,
  LastPx float64,
  Fee float64,
  StrategyId string
)
PARTITION BY to_start_of_hour(_tp_time)
TTL to_datetime(_tp_time) + INTERVAL 4 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

-- order
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.exchange_order_ext (
    raw string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_exchange_order',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'RawBLOB',
    one_message_per_row = true;

CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.exchange_order_w_ext (
    event_ts uint64,
    TradeDate date,
    OrderId string,
    SecurityExchange string,
    SecurityAccount string,
    SecurityId string,
    Symbol string,
    Side string,
    Quantity float64,
    Price float64,
    CumQuantity float64,
    OrdStatus string,
    StrategyId string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092',
    topic = 'invest_insights_exchange_order',
    security_protocol = 'SASL_SSL',
    sasl_mechanism = 'PLAIN',
    config_file = 'etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format = 'JSONEachRow',
    one_message_per_row = true;

CREATE STREAM IF NOT EXISTS invest_insights.exchange_order (
    event_ts uint64,
    TradeDate date,
    OrderId string,
    SecurityExchange string,
    SecurityAccount string,
    SecurityId string,
    Symbol string,
    Side string,
    Quantity float64,
    Price float64,
    CumQuantity float64,
    OrdStatus string,
    StrategyId string,
    _tp_time datetime64(6) default now64(6)
)
PARTITION BY to_YYYYMM(_tp_time)
TTL to_datetime(_tp_time) + INTERVAL 4 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


-- Pre-value table for storing last price
CREATE MUTABLE STREAM IF NOT EXISTS invest_insights.pre_value
(
  `SecurityAccount` string, `SecurityId` string, `prevalue` float64
)
PRIMARY KEY (SecurityAccount, SecurityId);
