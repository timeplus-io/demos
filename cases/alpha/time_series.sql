CREATE DATABASE IF NOT EXISTS alpha;

CREATE RANDOM STREAM alpha.market_data (
    timestamp datetime64(3) DEFAULT now64(3),
    symbol string DEFAULT array_element(
        ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM'], 
        (rand() % 8) + 1
    ),
    -- Price with realistic movements (log-normal around $150, ~2% volatility)
    price float64 DEFAULT round(150 * exp(rand_normal(0, 0.02)), 2),
    -- Bid-ask spread (0.01% to 0.1% of price)
    bid_price float64 DEFAULT round(price * (1 - rand_uniform(0.0001, 0.001)), 2),
    ask_price float64 DEFAULT round(price * (1 + rand_uniform(0.0001, 0.001)), 2),
    -- Volume with time-of-day patterns
    volume uint32 DEFAULT multi_if(
        to_hour(timestamp) >= 9 AND to_hour(timestamp) <= 10, rand_poisson(5000),  -- Market open
        to_hour(timestamp) >= 15 AND to_hour(timestamp) <= 16, rand_poisson(4000), -- Market close
        rand_poisson(1000)  -- Regular hours
    ),
    -- Market conditions
    volatility float32 DEFAULT round(rand_uniform(0.15, 0.35), 4),
    -- Trade direction indicator
    uptick bool DEFAULT rand_bernoulli(0.5)
) SETTINGS eps = 5, interval_time = 200;


-- Alpha 1: Momentum 
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
FROM alpha.market_data
PARTITION BY symbol;

-- Alpha 2: Mean Reversion
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
FROM alpha.market_data
PARTITION BY symbol;

-- Alpha 8: MA Crossover 
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
FROM alpha.market_data
PARTITION BY symbol;

-- Alpha 9: Price Acceleration
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
FROM alpha.market_data
PARTITION BY symbol;

-- Alpha 10: Bollinger Band Position
-- Calculate position within Bollinger Bands
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
FROM alpha.market_data
PARTITION BY symbol;

-- transactions

CREATE RANDOM STREAM alpha.transactions (
    timestamp datetime64(3) DEFAULT now64(3),
    transaction_id string DEFAULT concat('TXN-', to_string(rand64())),
    symbol string DEFAULT array_element(
        ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM'], 
        (rand() % 8) + 1
    ),
    -- Order type distribution
    order_type string DEFAULT array_element(
        ['MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT'], 
        (rand() % 4) + 1
    ),
    -- Side: 55% buy, 45% sell (slight bullish bias)
    side string DEFAULT multi_if(
        (rand() % 100) < 55, 'BUY',
        'SELL'
    ),
    -- Quantity with power-law distribution (most orders small, few large)
    quantity uint32 DEFAULT to_uint32(exp(rand_exponential(0.3)) * 100),
    -- Execution price
    execution_price float64 DEFAULT round(150 * exp(rand_normal(0, 0.02)), 2),
    -- Trader type
    trader_type string DEFAULT multi_if(
        (rand() % 100) < 70, 'RETAIL',      -- 70% retail
        (rand() % 100) < 90, 'INSTITUTIONAL', -- 20% institutional
        'HFT'                                -- 10% high-frequency
    ),
    -- Strategy identifier
    strategy_id string DEFAULT array_element(
        ['MOMENTUM', 'MEAN_REVERT', 'ARBITRAGE', 'MARKET_MAKING', 'TREND_FOLLOW'],
        (rand() % 5) + 1
    )
) SETTINGS eps = 50, interval_time = 10;

-- Combined Alpha Strategy
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
    FROM alpha.market_data
    PARTITION BY symbol
),
order_signals AS (
    SELECT
        symbol,
        (array_count(x -> x = 'BUY', group_array_last(side, 50)) - 
         array_count(x -> x = 'SELL', group_array_last(side, 50))) / 
        to_float64(50) AS order_imbalance
    FROM alpha.transactions
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
WITH alpha_stream AS
  (
    SELECT
      _tp_time, symbol, price, ((price - (lags(price, 1, 3)[3])) / (lags(price, 1, 3)[3])) - ((price - (lags(price, 1, 10)[10])) / (lags(price, 1, 10)[10])) AS momentum_alpha, (-1 * (price - array_avg(lags(price, 1, 20)))) / array_avg(lags(price, 1, 20)) AS mean_reversion_alpha, (array_avg(lags(price, 1, 5)) - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS ma_crossover_alpha
    FROM
      alpha.market_data
    PARTITION BY
      symbol
  )
SELECT
  window_start, symbol, avg(momentum_alpha) AS avg_momentum, avg(mean_reversion_alpha) AS avg_mean_reversion, avg(ma_crossover_alpha) AS avg_ma_cross, count(*) AS tick_count
FROM
  tumble(alpha_stream, 5s)
GROUP BY
  window_start, symbol