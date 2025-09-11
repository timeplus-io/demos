CREATE DATABASE IF NOT EXISTS invest_insights_data;

CREATE RANDOM STREAM IF NOT EXISTS invest_insights_data.order_random
(
  `order_idx` uint32 DEFAULT rand() % 40000000000,
  `account_idx` int32 DEFAULT rand() % 200,
  `security_idx` uint32 DEFAULT rand() % 3000,
  strategy_idx uint8 DEFAULT rand() % 8,
  side int8 DEFAULT rand() % 10,
  price_delta float64 default rand() % 10
)
SETTINGS eps = 1200;

-- target external streams
CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights_data.exchange_order_w_ext (
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

CREATE STREAM IF NOT EXISTS invest_insights_data.execution_w_ext (
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
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_execution', data_format='JSONEachRow', one_message_per_row=true;

CREATE EXTERNAL STREAM IF NOT EXISTS invest_insights_data.position_w_ext (
  event_ts uint64,
  TradeDate date,
  SecurityAccount string,
  SecurityId string,
  HoldingQty float64
)
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_position', data_format='JSONEachRow', one_message_per_row=true;


-- send data to kafka 
create materialized view if not exists invest_insights_data.i_order_src into invest_insights_data.exchange_order_w_ext as
select
    event_ts, TradeDate, concat('o', to_string(tuple_str.1)) as OrderId, SecurityExchange, SecurityAccount, SecurityId, Symbol,
    tuple_str.2 as Side, tuple_str.3 as Quantity,
    Price, (Quantity - price_delta * 10) as CumQuantity, OrdStatus, StrategyId
from (
    select
        to_unix_timestamp64_milli(now64(3)) as event_ts,
        today() as TradeDate,
        concat('o', order_idx::string) as OrderId,
        'US' as SecurityExchange,
        concat('a', account_idx::string) as SecurityAccount,
        to_string(100000 + security_idx % 200) as SecurityId,
        concat(SecurityId, '.', SecurityExchange) as Symbol,
        price_delta,
        array_join([tuple_cast(order_idx, '1', 500 + to_int64(price_delta * 10)), tuple_cast(order_idx+1, '2', 500)]) as tuple_str,
        100::float64 + price_delta as Price,
        '7' as OrdStatus,
        concat('sta', strategy_idx::string) as StrategyId
    from invest_insights_data.order_random
);

create materialized view if not exists invest_insights_data.i_execution_src into invest_insights_data.execution_w_ext as
select
    to_unix_timestamp64_milli(now64(3)) as event_ts,
    today() as TradeDate,
    concat('e', order_idx::string) as OrderId,
    concat('a', account_idx::string) as SecurityAccount,
    to_string(100000 + security_idx) as SecurityId,
    if(side>5, '2', '1') as EntrustDirection,
    500 + to_int64(price_delta * 10) as LastQty,
    100::float64 + price_delta as LastPx,
    0.0001 * LastQty * LastPx as Fee,
    concat('sta', strategy_idx::string) as StrategyId
from invest_insights_data.order_random;

create materialized view if not exists invest_insights_data.i_position_src into invest_insights_data.position_w_ext as
select
    to_unix_timestamp64_milli(now64(3)) as event_ts,
    today() as TradeDate,
    concat('a', account_idx::string) as SecurityAccount,
    to_string(100000 + security_idx) as SecurityId,
    5000 + to_int64(price_delta * 100) as HoldingQty
from invest_insights_data.order_random;

-- quote
CREATE RANDOM STREAM IF NOT EXISTS invest_insights_data.quote_random
(
  `security_idx` uint32 DEFAULT rand() % 3000,
  price_delta float64 default rand() % 10,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 2
)
SETTINGS eps = 3000;

CREATE STREAM IF NOT EXISTS invest_insights_data.stock_w_ext (
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
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_stock', data_format='JSONEachRow', one_message_per_row=true;


create materialized view if not exists invest_insights_data.i_quote_src into invest_insights_data.stock_w_ext as
select
    to_unix_timestamp64_milli(now64(3)) as event_ts,
    to_string(100000 + security_idx) as SecurityID,
    concat(SecurityID, '.US') as Symbol,
    100.0 as PreClosePx,
    100::float64 + price_delta as LastPx,
    99.0 as OpenPx,
    100.0 as ClosePx,
    100.0 as HighPx,
    90.0 as LowPx
from invest_insights_data.quote_random;
