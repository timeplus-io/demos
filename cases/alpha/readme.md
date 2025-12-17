# Comprehensive Query Explanation

This query calculates **5 different alpha signals** from market data and combines them into a single trading strategy. Let me break it down step by step.

---

## 🏗️ Query Structure

```
CTE 1: price_signals → Calculate 4 price-based alphas per symbol
CTE 2: order_signals → Calculate order flow imbalance per symbol
Final SELECT → Join signals and generate trading decisions
```

---

## 📊 Part 1: Price Signals CTE

### 1️⃣ Momentum Alpha
```sql
((price - array_element(lags(price, 3, 3), 1)) / array_element(lags(price, 3, 3), 1)) - 
((price - array_element(lags(price, 10, 10), 1)) / array_element(lags(price, 10, 10), 1)) AS momentum
```

**What it does:** Measures if short-term momentum is stronger than long-term momentum

**Step by step:**
```
Given prices: [150, 149, 148, 147, 146, ..., 140] (newest to oldest)

lags(price, 3, 3) = [147]  (price 3 bars ago)
lags(price, 10, 10) = [140] (price 10 bars ago)

Fast momentum (3-period):
= (150 - 147) / 147 = 3/147 = 0.0204 = +2.04%

Slow momentum (10-period):
= (150 - 140) / 140 = 10/140 = 0.0714 = +7.14%

Momentum alpha = 0.0204 - 0.0714 = -0.051 (NEGATIVE)
```

**Financial Intuition:**
- **Positive** → Short-term momentum > Long-term → Recent acceleration → **Bullish**
- **Negative** → Short-term momentum < Long-term → Losing steam → **Bearish**

**Example:**
- Stock rose 7% in 10 days, but only 2% in last 3 days → Momentum fading → Sell signal
- Stock rose 3% in 10 days, but 5% in last 3 days → Momentum building → Buy signal

---

### 2️⃣ Mean Reversion Alpha
```sql
-1 * (price - array_avg(lags(price, 1, 20))) / array_avg(lags(price, 1, 20)) AS mean_reversion
```

**What it does:** Measures how far the current price is from its 20-period average, betting it will revert

**Step by step:**
```
Given prices: [160, 155, 150, 148, 147, ..., 145] (newest to oldest)

lags(price, 1, 20) = [155, 150, 148, 147, ..., 145] (last 20 prices, excluding current)
array_avg(lags(price, 1, 20)) = 150 (average of past 20 bars)

Deviation = (160 - 150) / 150 = 0.0667 = +6.67% above average

Mean reversion alpha = -1 * 0.0667 = -0.0667 (NEGATIVE)
```

**Financial Intuition:**
- **Negative** → Price is above average → Expect reversion down → **Sell**
- **Positive** → Price is below average → Expect reversion up → **Buy**

**Example:**
- Price = $160, MA = $150 → +6.67% overbought → Signal = -0.0667 → Sell
- Price = $140, MA = $150 → -6.67% oversold → Signal = +0.0667 → Buy

---

### 3️⃣ MA Crossover Alpha
```sql
(array_avg(lags(price, 0, 4)) - array_avg(lags(price, 0, 19))) / array_avg(lags(price, 0, 19)) AS ma_cross
```

**What it does:** Classic "Golden Cross" - compares 5-period MA vs 20-period MA

**Step by step:**
```
Given prices: [150, 149, 148, 147, 146, ..., 140]

lags(price, 0, 4) = [150, 149, 148, 147, 146] (current + last 4)
MA_5 = array_avg([150, 149, 148, 147, 146]) = 148

lags(price, 0, 19) = [150, 149, ..., 131] (current + last 19)
MA_20 = array_avg([150, 149, ..., 131]) = 145

MA crossover = (148 - 145) / 145 = 3/145 = 0.0207 = +2.07%
```

**Financial Intuition:**
- **Positive** → Short MA > Long MA → **"Golden Cross"** → Bullish
- **Negative** → Short MA < Long MA → **"Death Cross"** → Bearish

**Example:**
- MA_5 = 148, MA_20 = 145 → Short-term trending above long-term → Buy
- MA_5 = 142, MA_20 = 145 → Short-term trending below long-term → Sell

---

### 4️⃣ Acceleration Alpha
```sql
((price - array_element(lags(price, 1, 1), 1)) - 
 (array_element(lags(price, 1, 1), 1) - array_element(lags(price, 2, 2), 1))) / price AS acceleration
```

**What it does:** Measures if price movement is accelerating (second derivative)

**Step by step:**
```
Given prices: [150, 148, 147, ...]

lags(price, 1, 1) = [148] (1 bar ago)
lags(price, 2, 2) = [147] (2 bars ago)

Velocity_current = 150 - 148 = +2 (price increased by 2)
Velocity_previous = 148 - 147 = +1 (price increased by 1)

Acceleration = 2 - 1 = +1 (velocity increased)
Normalized = 1 / 150 = 0.0067 = +0.67%
```

**Financial Intuition:**
- **Positive** → Price gains are accelerating → Strong momentum → **Bullish**
- **Negative** → Price gains are decelerating (or losses accelerating) → **Bearish**

**Example:**
- Day 1: +$1, Day 2: +$2 → Acceleration = +$1 → Momentum building → Buy
- Day 1: +$2, Day 2: +$1 → Acceleration = -$1 → Momentum fading → Sell

---

## 📈 Part 2: Order Signals CTE

```sql
(array_count(x -> x = 'BUY', group_array_last(side, 50)) - 
 array_count(x -> x = 'SELL', group_array_last(side, 50))) / 
to_float64(50) AS order_imbalance
```

**What it does:** Measures buy vs sell pressure from last 50 transactions

**Step by step:**
```
Last 50 transactions: [BUY, BUY, SELL, BUY, SELL, BUY, ...]

array_count(x -> x = 'BUY', last_50_sides) = 30
array_count(x -> x = 'SELL', last_50_sides) = 20

Order imbalance = (30 - 20) / 50 = 10/50 = 0.20 = +20%
```

**Financial Intuition:**
- **Positive** → More buy orders → Bullish pressure → **Buy**
- **Negative** → More sell orders → Bearish pressure → **Sell**

**Example:**
- 35 buys, 15 sells → Imbalance = +0.40 → Strong buying → Buy
- 15 buys, 35 sells → Imbalance = -0.40 → Strong selling → Sell

---

## 🎯 Part 3: Combined Alpha & Trading Signal

### Weighted Combination
```sql
combined_alpha = 
  0.25 * momentum +        -- 25% weight
  0.15 * mean_reversion +  -- 15% weight
  0.25 * ma_cross +        -- 25% weight
  0.15 * acceleration +    -- 15% weight
  0.20 * order_imbalance   -- 20% weight
```

**Why these weights?**
- **Momentum (25%)** & **MA Cross (25%)** → Trend-following signals (50% total)
- **Mean Reversion (15%)** → Counter-trend signal (diversification)
- **Acceleration (15%)** → Momentum confirmation
- **Order Flow (20%)** → Market microstructure insight

### Trading Logic
```sql
multi_if(
    combined_alpha > 0.01,  'BUY',   -- Above +1% threshold
    combined_alpha < -0.01, 'SELL',  -- Below -1% threshold
    'HOLD'                           -- Within [-1%, +1%]
)
```

---

## 💡 Complete Example Walkthrough

Let's say for **AAPL at 10:30:05 AM**:

### Price Signals:
```
momentum = +0.03 (short-term outperforming long-term, +3%)
mean_reversion = -0.02 (price above average by 2%, expect reversion)
ma_cross = +0.015 (5-day MA above 20-day MA by 1.5%)
acceleration = +0.01 (price gains accelerating, +1%)
```

### Order Signal:
```
order_imbalance = +0.15 (15% more buy orders than sell)
```

### Combined Alpha:
```
= 0.25(0.03) + 0.15(-0.02) + 0.25(0.015) + 0.15(0.01) + 0.20(0.15)
= 0.0075 + (-0.003) + 0.00375 + 0.0015 + 0.03
= 0.03975 = +3.975%
```

### Trading Signal:
```
0.03975 > 0.01 → BUY
```

**Interpretation:** Strong bullish signal - momentum building, golden cross present, strong buy pressure outweighs mean reversion concern.

---

## 🔍 Key Design Choices

### 1. PARTITION BY symbol
```sql
FROM alpha.market_data
PARTITION BY symbol
```
- Each symbol's alpha is calculated **independently**
- `lags()` only looks at history **within that symbol**
- Prevents mixing AAPL and GOOGL data

### 2. LEFT JOIN
```sql
FROM price_signals p
LEFT JOIN order_signals o ON p.symbol = o.symbol
```
- Keeps all price signals even if no order data exists
- `order_imbalance` will be NULL (treated as 0 in calculation)

### 3. Thresholds (±1%)
```
BUY: combined_alpha > 0.01
SELL: combined_alpha < -0.01
HOLD: [-0.01, 0.01]
```
- **Dead zone** prevents excessive trading
- Filters out noise, reduces transaction costs

---

## 🎓 Advanced Insights

### Signal Diversification
- **Trend signals** (momentum, MA cross) work in trending markets
- **Mean reversion** works in ranging markets
- **Combination balances both regimes**

### Real-time Processing
- Uses `PARTITION BY` for streaming computation
- No global state required (except `group_array_last` for orders)
- Can process millions of events per second

### Backtesting Potential
Could easily add:
```sql
-- Forward-looking return for validation
lead(price, 10) / price - 1 AS forward_return_10
```