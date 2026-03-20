CREATE DATABASE IF NOT EXISTS alpha;

CREATE STREAM market_data.coinbase_1s (
    window_start datetime64(3, 'UTC'),
    product_id string,
    -- OHLC
    open float64,
    high float64,
    low float64,
    close float64,
    -- Volume
    volume float64,
    buy_volume float64,
    sell_volume float64,
    trade_count uint64,
    -- Order book snapshot (last values in window)
    best_bid float64,
    best_ask float64,
    best_bid_size float64,
    best_ask_size float64,
    -- Derived
    spread float64,
    vwap float64
)
PARTITION BY to_start_of_hour(_tp_time)
TTL to_datetime(_tp_time) + INTERVAL 4 HOUR
SETTINGS index_granularity = 8192, logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


CREATE MATERIALIZED VIEW market_data.mv_coinbase_1s 
INTO market_data.coinbase_1s AS
SELECT 
    window_start as time,
    product_id,
    open,
    high,
    low,
    close,
    volume,
    buy_volume,
    sell_volume,
    trade_count,
    best_bid,
    best_ask,
    best_bid_size,
    best_ask_size,
    -- Now these reference column aliases, not nested aggregates
    best_ask - best_bid AS spread,
    total_cost / (volume + 0.0001) AS vwap
FROM (
    SELECT 
        window_start,
        product_id,
        earliest(price) AS open,
        max(price) AS high,
        min(price) AS low,
        latest(price) AS close,
        sum(last_size) AS volume,
        sum(if(side = 'buy', last_size, 0)) AS buy_volume,
        sum(if(side = 'sell', last_size, 0)) AS sell_volume,
        count(*) AS trade_count,
        latest(best_bid) AS best_bid,
        latest(best_ask) AS best_ask,
        latest(best_bid_size) AS best_bid_size,
        latest(best_ask_size) AS best_ask_size,
        sum(price * last_size) AS total_cost
    FROM tumble(market_data.coinbase_tickers, 1s)
    GROUP BY window_start, product_id
);

-- alphas

CREATE OR REPLACE VIEW alpha.v_momentum AS
SELECT 
    product_id,
    time,
    close,
    (close - array_element(lags(close, 3, 3), 1)) 
        / array_element(lags(close, 3, 3), 1) AS return_3s,
    (close - array_element(lags(close, 10, 10), 1)) 
        / array_element(lags(close, 10, 10), 1) AS return_10s,
    ((close - array_element(lags(close, 3, 3), 1)) / array_element(lags(close, 3, 3), 1))
    - ((close - array_element(lags(close, 10, 10), 1)) / array_element(lags(close, 10, 10), 1)) 
        AS momentum_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_mean_reversion AS
SELECT 
    product_id,
    time,
    close,
    array_avg(lags(close, 1, 20)) AS ma_20s,
    (close - array_avg(lags(close, 1, 20))) 
        / array_avg(lags(close, 1, 20)) AS deviation,
    -1 * (close - array_avg(lags(close, 1, 20))) 
        / array_avg(lags(close, 1, 20)) AS mean_reversion_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_spread_pressure AS
SELECT 
    product_id,
    time,
    close,
    best_bid,
    best_ask,
    spread * 10000 / close AS spread_bps,
    -- Trade location: where did close land vs midpoint
    (close - (best_bid + best_ask) / 2) 
        / (spread + 0.0001) AS trade_location_alpha,
    -- Book imbalance
    (best_bid_size - best_ask_size) 
        / (best_bid_size + best_ask_size + 0.0001) AS book_imbalance_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_ma_crossover AS
SELECT 
    product_id,
    time,
    close,
    array_avg(lags(close, 0, 4)) AS ma_5s,
    array_avg(lags(close, 0, 19)) AS ma_20s,
    (array_avg(lags(close, 0, 4)) - array_avg(lags(close, 0, 19))) 
        / array_avg(lags(close, 0, 19)) AS ma_crossover_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_price_acceleration AS
SELECT 
    product_id,
    time,
    close,
    (close - array_element(lags(close, 1, 1), 1)) AS velocity_now,
    (array_element(lags(close, 1, 1), 1) - array_element(lags(close, 2, 2), 1)) AS velocity_prev,
    ((close - array_element(lags(close, 1, 1), 1)) 
     - (array_element(lags(close, 1, 1), 1) - array_element(lags(close, 2, 2), 1))) 
        / close AS acceleration_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_bollinger_position AS
SELECT 
    product_id,
    time,
    close,
    array_avg(lags(close, 1, 20)) AS mean_20s,
    sqrt(array_avg(
        array_map(x -> pow(x - array_avg(lags(close, 1, 20)), 2), lags(close, 1, 20))
    )) AS std_dev,
    (close - array_avg(lags(close, 1, 20))) 
        / (2 * sqrt(array_avg(
            array_map(x -> pow(x - array_avg(lags(close, 1, 20)), 2), lags(close, 1, 20))
        )) + 0.0001) AS bollinger_position_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_vwap_alpha AS
SELECT 
    product_id,
    time,
    close,
    vwap,
    -- Rolling VWAP over 20s (volume-weighted average of 1s VWAPs)
    array_sum(array_map(
        (p, v) -> p * v, 
        lags(vwap, 0, 19), 
        lags(volume, 0, 19)
    )) / (array_sum(lags(volume, 0, 19)) + 0.0001) AS vwap_20s,
    -- Close vs rolling VWAP
    (close - array_sum(array_map(
        (p, v) -> p * v, 
        lags(vwap, 0, 19), 
        lags(volume, 0, 19)
    )) / (array_sum(lags(volume, 0, 19)) + 0.0001)) 
        / close AS vwap_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_trade_flow AS
SELECT 
    product_id,
    time,
    close,
    buy_volume,
    sell_volume,
    -- Rolling 20s order flow imbalance
    array_sum(lags(buy_volume, 0, 19)) AS buy_vol_20s,
    array_sum(lags(sell_volume, 0, 19)) AS sell_vol_20s,
    (array_sum(lags(buy_volume, 0, 19)) - array_sum(lags(sell_volume, 0, 19))) 
        / (array_sum(lags(volume, 0, 19)) + 0.0001) AS trade_flow_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_spread_regime AS
SELECT 
    product_id,
    time,
    close,
    spread,
    array_avg(lags(spread, 1, 20)) AS avg_spread_20s,
    spread / (array_avg(lags(spread, 1, 20)) + 0.0001) AS spread_ratio,
    -- Contrarian: buy when spread widens (panic)
    -1 * (spread / (array_avg(lags(spread, 1, 20)) + 0.0001) - 1) AS spread_regime_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_volume_spike AS
SELECT 
    product_id,
    time,
    close,
    volume,
    array_avg(lags(volume, 1, 20)) AS avg_volume_20s,
    -- Volume ratio: >2 = spike
    volume / (array_avg(lags(volume, 1, 20)) + 0.0001) AS volume_ratio,
    -- Volume spike + direction: high volume + price up = bullish
    (volume / (array_avg(lags(volume, 1, 20)) + 0.0001)) 
        * ((close - array_element(lags(close, 1, 1), 1)) / close) AS volume_spike_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_composite AS
SELECT 
    product_id,
    time,
    close,
    -- Momentum
    (close - array_element(lags(close, 3, 3), 1)) 
        / array_element(lags(close, 3, 3), 1) AS momentum,
    -- Mean reversion
    -1 * (close - array_avg(lags(close, 1, 20))) 
        / array_avg(lags(close, 1, 20)) AS mean_rev,
    -- Book imbalance
    (best_bid_size - best_ask_size) 
        / (best_bid_size + best_ask_size + 0.0001) AS book_imb,
    -- Trade flow
    (buy_volume - sell_volume) 
        / (volume + 0.0001) AS flow,
    -- Equal-weighted composite
    (
        (close - array_element(lags(close, 3, 3), 1)) / array_element(lags(close, 3, 3), 1)
        + (-1 * (close - array_avg(lags(close, 1, 20))) / array_avg(lags(close, 1, 20)))
        + (best_bid_size - best_ask_size) / (best_bid_size + best_ask_size + 0.0001)
        + (buy_volume - sell_volume) / (volume + 0.0001)
    ) / 4 AS composite_alpha
FROM market_data.coinbase_1s
PARTITION BY product_id;

