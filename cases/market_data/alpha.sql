CREATE DATABASE IF NOT EXISTS alpha;

CREATE OR REPLACE VIEW alpha.v_momentum AS
SELECT 
    product_id,
    _tp_time,
    price,
    -- 10-tick return
    (price - array_element(lags(price, 10, 10), 1)) 
        / array_element(lags(price, 10, 10), 1) AS return_10,
    -- 3-tick return
    (price - array_element(lags(price, 3, 3), 1)) 
        / array_element(lags(price, 3, 3), 1) AS return_3,
    -- Momentum alpha: fast minus slow
    ((price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1))
    - ((price - array_element(lags(price, 10, 10), 1)) / array_element(lags(price, 10, 10), 1)) 
        AS momentum_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_mean_reversion AS
SELECT 
    product_id,
    _tp_time,
    price,
    array_avg(lags(price, 1, 20)) AS ma_20,
    (price - array_avg(lags(price, 1, 20))) 
        / array_avg(lags(price, 1, 20)) AS deviation,
    -- Negative deviation = buy signal (price below mean reverts up)
    -1 * (price - array_avg(lags(price, 1, 20))) 
        / array_avg(lags(price, 1, 20)) AS mean_reversion_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_spread_pressure AS
SELECT 
    product_id,
    _tp_time,
    price,
    best_bid,
    best_ask,
    -- Spread in basis points
    (best_ask - best_bid) / price * 10000 AS spread_bps,
    -- Midpoint vs last trade: positive = traded above mid (buying pressure)
    (price - (best_bid + best_ask) / 2) 
        / ((best_ask - best_bid) + 0.0001) AS trade_location_alpha,
    -- Size imbalance: positive = more bid support
    (best_bid_size - best_ask_size) 
        / (best_bid_size + best_ask_size + 0.0001) AS book_imbalance_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_ma_crossover AS
SELECT 
    product_id,
    _tp_time,
    price,
    array_avg(lags(price, 0, 4)) AS ma_5,
    array_avg(lags(price, 0, 19)) AS ma_20,
    (array_avg(lags(price, 0, 4)) - array_avg(lags(price, 0, 19))) 
        / array_avg(lags(price, 0, 19)) AS ma_crossover_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_price_acceleration AS
SELECT 
    product_id,
    _tp_time,
    price,
    (price - array_element(lags(price, 1, 1), 1)) AS velocity_now,
    (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1)) AS velocity_prev,
    -- Acceleration normalized by price
    ((price - array_element(lags(price, 1, 1), 1)) 
     - (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1))) 
        / price AS acceleration_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_bollinger_position AS
SELECT 
    product_id,
    _tp_time,
    price,
    array_avg(lags(price, 1, 20)) AS mean_20,
    sqrt(array_avg(
        array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20))
    )) AS std_dev,
    -- Position within bands: ~[-1, +1], beyond = breakout
    (price - array_avg(lags(price, 1, 20))) 
        / (2 * sqrt(array_avg(
            array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20))
        )) + 0.0001) AS bollinger_position_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;


CREATE OR REPLACE VIEW alpha.v_vwap_alpha AS
SELECT 
    product_id,
    _tp_time,
    price,
    last_size,
    -- Rolling VWAP over 20 ticks
    array_sum(array_map(
        (p, v) -> p * v, 
        lags(price, 0, 19), 
        lags(last_size, 0, 19)
    )) / (array_sum(lags(last_size, 0, 19)) + 0.0001) AS vwap_20,
    -- Price vs VWAP: positive = trading above fair value
    (price - array_sum(array_map(
        (p, v) -> p * v, 
        lags(price, 0, 19), 
        lags(last_size, 0, 19)
    )) / (array_sum(lags(last_size, 0, 19)) + 0.0001)) 
        / price AS vwap_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

CREATE OR REPLACE VIEW alpha.v_trade_flow AS
SELECT 
    product_id,
    _tp_time,
    price,
    side,
    -- Buy volume over last 20 ticks
    array_sum(array_map(
        (s, v) -> if(s = 'buy', v, 0), 
        lags(side, 0, 19), 
        lags(last_size, 0, 19)
    )) AS buy_volume_20,
    -- Sell volume over last 20 ticks
    array_sum(array_map(
        (s, v) -> if(s = 'sell', v, 0), 
        lags(side, 0, 19), 
        lags(last_size, 0, 19)
    )) AS sell_volume_20,
    -- Order flow imbalance alpha: positive = net buying
    (array_sum(array_map(
        (s, v) -> if(s = 'buy', v, 0), 
        lags(side, 0, 19), lags(last_size, 0, 19)
    )) - array_sum(array_map(
        (s, v) -> if(s = 'sell', v, 0), 
        lags(side, 0, 19), lags(last_size, 0, 19)
    ))) / (array_sum(lags(last_size, 0, 19)) + 0.0001) AS trade_flow_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;


CREATE OR REPLACE VIEW alpha.v_spread_regime AS
SELECT 
    product_id,
    _tp_time,
    price,
    (best_ask - best_bid) AS spread,
    array_avg(lags(best_ask - best_bid, 1, 20)) AS avg_spread_20,
    -- Spread expansion ratio: >1 = widening (risk-off), <1 = tightening (risk-on)
    (best_ask - best_bid) 
        / (array_avg(lags(best_ask - best_bid, 1, 20)) + 0.0001) AS spread_ratio,
    -- Alpha: contrarian on spread expansion (buy when others panic)
    -1 * ((best_ask - best_bid) 
        / (array_avg(lags(best_ask - best_bid, 1, 20)) + 0.0001) - 1) AS spread_regime_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;


CREATE OR REPLACE VIEW alpha.v_composite AS
SELECT 
    product_id,
    _tp_time,
    price,
    -- Momentum component (fast return)
    (price - array_element(lags(price, 3, 3), 1)) 
        / array_element(lags(price, 3, 3), 1) AS momentum,
    -- Mean reversion component
    -1 * (price - array_avg(lags(price, 1, 20))) 
        / array_avg(lags(price, 1, 20)) AS mean_rev,
    -- Book imbalance component
    (best_bid_size - best_ask_size) 
        / (best_bid_size + best_ask_size + 0.0001) AS book_imb,
    -- Equal-weighted composite
    (
        (price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1)
        + (-1 * (price - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)))
        + (best_bid_size - best_ask_size) / (best_bid_size + best_ask_size + 0.0001)
    ) / 3 AS composite_alpha
FROM market_data.coinbase_tickers
PARTITION BY product_id;

