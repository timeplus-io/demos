CREATE DATABASE IF NOT EXISTS market_data;

-- python table function to read from coinbase websocket feed
SYSTEM INSTALL PYTHON PACKAGE 'json5>=0.9.6';
SYSTEM INSTALL PYTHON PACKAGE 'websocket-client>=1.4.0';

CREATE EXTERNAL STREAM market_data.coinbase_websocket_read_connector(type string, product_id string, channel string, full_payload string, received_at datetime64(3))
AS $$
import websocket
import json5
import time
from datetime import datetime

def read_coinbase_websocket_stream():
    websocket_url = "wss://ws-feed.exchange.coinbase.com"
    subscription_message = '{"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}'

    ws = None
    while True:
        try:
            ws = websocket.create_connection(websocket_url)
            ws.send(subscription_message)

            while True:
                message = ws.recv() or ""
                parsed_message = json5.loads(message) or {}

                msg_type = parsed_message.get("type") or ""
                product_id = parsed_message.get("product_id") or ""

                channel_name = ""
                channels = parsed_message.get("channels")
                if msg_type == "subscriptions" and channels:
                    channel_name = ", ".join([c.get("name", "unknown") for c in channels]) or ""
                elif "channel" in parsed_message:
                    channel_name = parsed_message.get("channel") or ""

                yield (
                    msg_type,
                    product_id,
                    channel_name,
                    message,
                    datetime.utcnow(),
                )

        except Exception:
            time.sleep(5)
        finally:
            if ws:
                ws.close()
        time.sleep(1)

$$
SETTINGS type='python', mode='streaming', read_function_name='read_coinbase_websocket_stream';

-- tickers stream created by source
CREATE STREAM market_data.coinbase_tickers
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
  `best_bid_size` float64
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

CREATE MATERIALIZED VIEW market_data.mv_coinbase_tickers_extracted
INTO market_data.coinbase_tickers
AS
SELECT 
  full_payload:best_ask::float as best_ask,
  full_payload:product_id as product_id,
  full_payload:price::float as price,
  full_payload:trade_id::float as trade_id,
  full_payload:best_bid::float as best_bid,
  full_payload:open_24h::float as open_24h,
  full_payload:sequence::float as sequence,
  full_payload:volume_30d::float as volume_30d,
  full_payload:high_24h::float as high_24h,
  full_payload:low_24h::float as low_24h,
  full_payload:last_size::float as last_size,
  full_payload:side as side,
  full_payload:time as time,
  full_payload:type as type,
  full_payload:volume_24h::float as volume_24h,
  full_payload:best_ask_size::float as best_ask_size,
  full_payload:best_bid_size::float as best_bid_size,
  to_time(time) as _tp_time
FROM market_data.coinbase_websocket_read_connector
WHERE full_payload:type = 'ticker'


CREATE VIEW market_data.v_coinbase_btc_ohlc_1m
AS
SELECT
  window_start, earliest(price) AS open, latest(price) AS close, max(price) AS high, min(price) AS low
FROM
  tumble(market_data.coinbase_tickers, 1m)
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
  tumble(market_data.coinbase_tickers, 1m)
WHERE
  _tp_time > (now() - 1h)
GROUP BY
  window_start, product_id
EMIT STREAM PERIODIC 250ms


