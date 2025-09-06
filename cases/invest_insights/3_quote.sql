CREATE STREAM IF NOT EXISTS invest_insights.stock_ext (
    `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_stock';

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
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'invest_insights_stock', data_format='JSONEachRow', one_message_per_row=true;

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
PRIMARY KEY SecurityID;
