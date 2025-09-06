CREATE DATABASE IF NOT EXISTS market_data;

-- tickers stream created by source
CREATE STREAM default.coinbase_tickers
(
  `best_ask` float64,
  `product_id` string,
  `price` float64,
  `trade_id` float64,
  `best_bid` float64,
  `open_24h` float64,
  `sequence` float64,
  `volume_30d` float64,
  `high_24h` float64,
  `low_24h` float64,
  `last_size` float64,
  `side` string,
  `time` string,
  `type` string,
  `volume_24h` float64,
  `best_ask_size` float64,
  `best_bid_size` float64,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  `_tp_sn` int64 CODEC(Delta(8), ZSTD(1)),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 32,
  INDEX _tp_sn_index _tp_sn TYPE minmax GRANULARITY 32
)
ENGINE = Stream(1, 3, rand())
PARTITION BY to_YYYYMM(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
TTL to_datetime(_tp_time) + INTERVAL 1 DAY
SETTINGS mode = 'append', logstore_retention_bytes = '-1', logstore_retention_ms = '86400000', index_granularity = 8192
COMMENT ' '

CREATE MUTABLE STREAM market_data.coinbase_ohlc_1m_vkv
(
  `time` datetime64(3),
  `symbol` string,
  `open` float32,
  `close` float32,
  `high` float32,
  `low` float32,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
PRIMARY KEY (time, symbol);

-- views
CREATE VIEW market_data.bitcoin_usd
AS
SELECT
  *
FROM
  default.coinbase_tickers
WHERE
  product_id = 'BTC-USD';

-- MV
CREATE MATERIALIZED VIEW market_data.mv_coinbase_tickers_extracted
AS
SELECT
  cast(raw:best_ask, 'float') AS best_ask, 
  cast(raw:best_ask_size, 'float') AS best_ask_size, 
  cast(raw:best_bid, 'float') AS best_bid, 
  cast(raw:best_bid_size, 'float') AS best_bid_size, 
  cast(raw:high_24h, 'float') AS high_24h, 
  cast(raw:last_size, 'float') AS last_size, 
  cast(raw:low_24h, 'float') AS low_24h, 
  cast(raw:open_24h, 'float') AS open_24h, 
  cast(raw:price, 'float') AS price, 
  cast(raw:sequence, 'bigint') AS sequence, raw:side AS side, 
  cast(raw:trade_id, 'bigint') AS trade_id, raw:type AS type, 
  cast(raw:volume_24h, 'float') AS volume_24h, 
  cast(raw:volume_30d, 'float') AS volume_30d, 
  raw:product_id AS product_id, 
  to_time(raw:time) AS _tp_time
FROM
  market_data.coinbase_tickers
WHERE
  _tp_time > earliest_timestamp()
STORAGE_SETTINGS index_granularity = 8192, logstore_retention_bytes = -1, logstore_retention_ms = 86400000
TTL to_datetime(_tp_time) + INTERVAL 30 DAY;


CREATE VIEW market_data.v_coinbase_btc_ohlc_1m
AS
SELECT
  window_start, earliest(price) AS open, latest(price) AS close, max(price) AS high, min(price) AS low
FROM
  tumble(coinbase_tickers, 1m)
WHERE
  (product_id = 'BTC-USD') AND (_tp_time > (now() - 1h))
GROUP BY
  window_start;

CREATE VIEW market_data.v_coinbase_btc_1m_ret
AS
SELECT
  window_start AS time, close, lag(close) AS prev_close, (close - prev_close) / prev_close AS ret
FROM
  market_data.v_coinbase_btc_ohlc_1m
WHERE
  prev_close > 0;

CREATE VIEW market_data.v_coinbase_btc_1m_rsi
AS
SELECT
  time, 
  lags(ret, 1, 14) AS rets, 
  array_avg(array_map(x -> if(x > 0, x, 0), rets)) AS avg_gains, 
  array_avg(array_map(x -> if(x > 0, 0, -x), rets)) AS avg_losses, 
  avg_gains / avg_losses AS RS, 100 - (100 / (1 + RS)) AS RSI
FROM
  market_data.v_coinbase_btc_1m_ret;


CREATE MATERIALIZED VIEW market_data.mv_ohlc_by_symbol INTO market_data.coinbase_ohlc_1m_vkv
AS
SELECT
  window_start AS time, 
  product_id AS symbol, 
  earliest(price) AS open, 
  latest(price) AS close, 
  max(price) AS high, 
  min(price) AS low
FROM
  tumble(coinbase_tickers, 1m)
WHERE
  _tp_time > (now() - 1h)
GROUP BY
  window_start, product_id
EMIT STREAM PERIODIC 250ms


