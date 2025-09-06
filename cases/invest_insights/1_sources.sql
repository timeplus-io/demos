CREATE DATABASE IF NOT EXISTS invest_insights;

-- position
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.position_ext (
    raw string
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_position', data_format='RawBLOB', one_message_per_row=true;

CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.position_w_ext (
  event_ts uint64,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  HoldingQty float64
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_position', data_format='JSONEachRow', one_message_per_row=true;

CREATE Mutable STREAM IF NOT EXISTS invest_insights.position (
  event_ts uint64,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  HoldingQty float64
)
PRIMARY KEY(SecurityAccount, SecurityId);

-- execution
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.execution_ext (
    raw string
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_execution';

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
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_execution', data_format='JSONEachRow', one_message_per_row=true;

CREATE STREAM IF NOT EXISTS invest_insights.execution (
  event_ts uint64,
  OrderId string,
  TradeDate date,
  SecurityAccount string,
  SecurityId low_cardinality(string),
  EntrustDirection string,
  LastQty float64,
  LastPx float64,
  Fee float64,
  StrategyId low_cardinality(string)
)
PARTITION BY to_start_of_hour(_tp_time)
TTL to_datetime(_tp_time) + INTERVAL 1 DAY;

-- order
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights.exchange_order_ext (
    raw string
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_exchange_order';

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
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_exchange_order', data_format='JSONEachRow', one_message_per_row=true;

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
TTL to_datetime(_tp_time) + INTERVAL 1 DAY;


-- Pre-value table for storing last price
CREATE MUTABLE STREAM IF NOT EXISTS invest_insights.pre_value
(
  `SecurityAccount` string, `SecurityId` string, `prevalue` float64
)
PRIMARY KEY (SecurityAccount, SecurityId);


-- update retention policy for streams
ALTER STREAM invest_insights.exchange_order MODIFY SETTING
logstore_retention_ms = '3600000',
logstore_retention_bytes = '107374182';

ALTER STREAM invest_insights.execution MODIFY SETTING
logstore_retention_ms = '3600000',
logstore_retention_bytes = '107374182';