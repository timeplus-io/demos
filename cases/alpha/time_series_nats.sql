-- read from NATS streams

CREATE VIEW alpha.v_market_data_parsed AS
SELECT 
    to_datetime64(message:timestamp, 3) as timestamp,
    message:symbol as symbol,
    message:price::float as price,
    message:bid_price::float as bid_price,
    message:ask_price::float as ask_price,
    message:volume::float as volume,
    message:volatility::float as volatility,
    message:uptick::bool as uptick
FROM market_data_from_nats;

CREATE VIEW alpha.v_transactions_parsed AS
SELECT
    to_datetime64(message:timestamp, 3) as timestamp,
    message:transaction_id as transaction_id,
    message:symbol as symbol,
    message:order_type as order_type,
    message:side as side,
    message:quantity::float as quantity,
    message:execution_price::float as execution_price,
    message:trader_type as trader_type,
    message:strategy_id as strategy_id
FROM transactions_from_nats

-- alphas

-- Alpha 1: Momentum
CREATE OR REPLACE VIEW alpha.v_momentum AS
SELECT 
    symbol,
    timestamp,
    price,
    -- 10-period return: current vs 10 bars ago
    (price - array_element(lags(price, 10, 10), 1)) / array_element(lags(price, 10, 10), 1) AS return_10,
    -- 3-period return: current vs 3 bars ago
    (price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1) AS return_3,
    -- Momentum alpha: fast - slow
    ((price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1)) - 
    ((price - array_element(lags(price, 10, 10), 1)) / array_element(lags(price, 10, 10), 1)) AS momentum_alpha
FROM alpha.v_market_data_parsed
PARTITION BY symbol;

-- Alpha 2: Mean Reversion
CREATE OR REPLACE VIEW alpha.v_mean_reversion AS
SELECT 
    symbol,
    timestamp,
    price,
    -- MA of past 20 bars (not including current)
    array_avg(lags(price, 1, 20)) AS ma_20_past,
    -- Current price deviation from past MA
    (price - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS deviation,
    -- Mean reversion alpha
    -1 * (price - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS mean_reversion_alpha
FROM alpha.v_market_data_parsed
PARTITION BY symbol;

-- Alpha 8: MA Crossover 
CREATE OR REPLACE VIEW alpha.v_ma_crossover AS
SELECT 
    symbol,
    timestamp,
    price,
    -- Short MA (5 periods including current)
    array_avg(lags(price, 0, 4)) AS ma_5,
    -- Long MA (20 periods including current)
    array_avg(lags(price, 0, 19)) AS ma_20,
    -- MA crossover alpha
    (array_avg(lags(price, 0, 4)) - array_avg(lags(price, 0, 19))) / array_avg(lags(price, 0, 19)) AS ma_crossover_alpha
FROM alpha.v_market_data_parsed
PARTITION BY symbol;

-- Alpha 9: Price Acceleration
CREATE OR REPLACE VIEW alpha.v_price_acceleration AS
SELECT 
    symbol,
    timestamp,
    price,
    -- Velocity: current vs 1 bar ago
    (price - array_element(lags(price, 1, 1), 1)) AS velocity_current,
    -- Previous velocity: 1 bar ago vs 2 bars ago
    (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1)) AS velocity_previous,
    -- Acceleration: change in velocity
    (price - array_element(lags(price, 1, 1), 1)) - 
    (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1)) AS acceleration,
    -- Normalized acceleration alpha
    ((price - array_element(lags(price, 1, 1), 1)) - 
     (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1))) / price AS acceleration_alpha
FROM alpha.v_market_data_parsed
PARTITION BY symbol;


-- Alpha 10: Bollinger Band Position
-- Calculate position within Bollinger Bands
CREATE OR REPLACE VIEW alpha.v_bollinger_position AS
SELECT 
    symbol,
    timestamp,
    price,
    lags(price, 1, 20) AS price_history,
    array_avg(lags(price, 1, 20)) AS mean_price,
    -- Standard deviation
    sqrt(array_avg(array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20)))) AS std_dev,
    -- Bollinger bands
    array_avg(lags(price, 1, 20)) - 2 * sqrt(array_avg(array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20)))) AS lower_band,
    array_avg(lags(price, 1, 20)) + 2 * sqrt(array_avg(array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20)))) AS upper_band,
    -- Position alpha: -1 at lower band, +1 at upper band, 0 at mean
    (price - array_avg(lags(price, 1, 20))) / 
    (2 * sqrt(array_avg(array_map(x -> pow(x - array_avg(lags(price, 1, 20)), 2), lags(price, 1, 20)))) + 0.0001) AS bollinger_position_alpha
FROM alpha.v_market_data_parsed
PARTITION BY symbol;


-- Combined Alpha Strategy
CREATE OR REPLACE VIEW alpha.v_price_signals AS
WITH price_signals AS (
    SELECT 
        symbol,
        timestamp,
        price,
        -- Momentum: current vs lags (clean indexing)
        ((price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1)) - 
        ((price - array_element(lags(price, 10, 10), 1)) / array_element(lags(price, 10, 10), 1)) AS momentum,
        -- Mean reversion: current vs MA of past
        -1 * (price - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS mean_reversion,
        -- MA crossover: standard MAs including current
        (array_avg(lags(price, 0, 4)) - array_avg(lags(price, 0, 19))) / array_avg(lags(price, 0, 19)) AS ma_cross,
        -- Acceleration
        ((price - array_element(lags(price, 1, 1), 1)) - 
         (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1))) / price AS acceleration
    FROM alpha.v_market_data_parsed
    PARTITION BY symbol
),
order_signals AS (
    SELECT
        symbol,
        (array_count(x -> x = 'BUY', group_array_last(side, 50)) - 
         array_count(x -> x = 'SELL', group_array_last(side, 50))) / 
        to_float64(50) AS order_imbalance
    FROM alpha.v_transactions_parsed
    GROUP BY symbol
)
SELECT 
    p.symbol,
    p.timestamp,
    p.price,
    p.momentum,
    p.mean_reversion,
    p.ma_cross,
    p.acceleration,
    o.order_imbalance,
    (0.25 * p.momentum + 
     0.15 * p.mean_reversion + 
     0.25 * p.ma_cross + 
     0.15 * p.acceleration +
     0.20 * o.order_imbalance) AS combined_alpha,
    multi_if(
        (0.25 * p.momentum + 0.15 * p.mean_reversion + 0.25 * p.ma_cross + 0.15 * p.acceleration + 0.20 * o.order_imbalance) > 0.01, 'BUY',
        (0.25 * p.momentum + 0.15 * p.mean_reversion + 0.25 * p.ma_cross + 0.15 * p.acceleration + 0.20 * o.order_imbalance) < -0.01, 'SELL',
        'HOLD'
    ) AS trading_signal
FROM price_signals p
LEFT JOIN order_signals o ON p.symbol = o.symbol;

-- monitor queries
-- Real-time Alpha Dashboard
CREATE OR REPLACE VIEW alpha.v_alpha_dashboard AS
WITH alpha_stream AS
  (
    SELECT
      timestamp, symbol, price, ((price - (lags(price, 1, 3)[3])) / (lags(price, 1, 3)[3])) - ((price - (lags(price, 1, 10)[10])) / (lags(price, 1, 10)[10])) AS momentum_alpha, (-1 * (price - array_avg(lags(price, 1, 20)))) / array_avg(lags(price, 1, 20)) AS mean_reversion_alpha, (array_avg(lags(price, 1, 5)) - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS ma_crossover_alpha
    FROM
      alpha.v_market_data_parsed
    PARTITION BY
      symbol
  )
SELECT
  window_start, symbol, avg(momentum_alpha) AS avg_momentum, avg(mean_reversion_alpha) AS avg_mean_reversion, avg(ma_crossover_alpha) AS avg_ma_cross, count(*) AS tick_count
FROM
  tumble(alpha_stream, timestamp, 5s)
GROUP BY
  window_start, symbol




