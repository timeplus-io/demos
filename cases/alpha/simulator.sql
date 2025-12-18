CREATE DATABASE IF NOT EXISTS data;

CREATE RANDOM STREAM data.market_data (
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


CREATE RANDOM STREAM data.transactions (
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
