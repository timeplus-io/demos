# Timeplus Real-Time Trading Monitoring System

This project demonstrates a comprehensive real-time trading activity monitoring system built with Timeplus. The system tracks order execution, position management, market data, and calculates key trading metrics including participation rates and profit/loss in real-time.

## Architecture Overview

The system processes multiple data streams through Kafka and performs real-time analytics:
- **Order Management**: Tracks order lifecycle and execution
- **Position Tracking**: Monitors security holdings across accounts
- **Market Data**: Processes real-time stock quotes
- **Metrics Calculation**: Computes participation rates and P&L

## File Structure and Execution Order

Execute the SQL files in numerical order (0-7) to set up the complete system:

### 0_generator.sql - Data Generation Layer
**Purpose**: Creates synthetic trading data for testing and demonstration

**Key Components**:
- **Random Streams**: Generates realistic trading patterns
  - `order_random`: Creates order flow with configurable rates (1200 EPS)
  - `quote_random`: Generates market data updates (3000 EPS)

- **Data Mapping**: Transforms random data into realistic trading events
  - Order generation with buy/sell sides, quantities, and prices
  - Execution records with fees and strategy assignments
  - Position updates reflecting holdings changes
  - Stock price movements with OHLC data

**Example Output**:
```sql
-- Generates orders like:
OrderId: o12345, Side: 1 (Buy), Quantity: 510, Price: 105.5
SecurityId: 600001, Account: a123, Strategy: sta2
```

### 1_sources.sql - Data Ingestion Infrastructure
**Purpose**: Establishes Kafka-based data ingestion for all trading events

**Stream Definitions**:

**Position Streams**:
- `position_ext`: Raw position data from Kafka
- `position`: Processed holdings by account and security
- Primary Key: `(SecurityAccount, SecurityId)`

**Execution Streams**:
- `execution_ext`: Raw execution data
- `execution`: Trade execution records with P&L impact
- TTL: 1 day (for recent trade analysis)

**Order Streams**:
- `exchange_order_ext`: Raw order flow
- `exchange_order`: Complete order lifecycle tracking
- Partitioned by month for efficient querying

**Key Features**:
- **Retention Policies**: Optimized for real-time analysis (1 hour logstore)
- **Partitioning**: Time-based partitioning for performance
- **Primary Keys**: Ensures data consistency and efficient updates

### 2_cfg.sql - Configuration Management
**Purpose**: Stores trading parameters and risk limits

**Configuration Parameters**:
- `minSpread`: Minimum bid-ask spread requirements
- `minReportBalance`: Minimum order size for participation calculation
- `securityPosition`: Position limits per security
- `callAuctionRatio` / `continousAuctionRatio`: Auction participation thresholds
- `execBalanceRequire`: Execution balance requirements
- `canceledReportRatio`: Order cancellation limits

**Usage**: These parameters control the behavior of trading algorithms and risk management systems.

### 3_quote.sql - Market Data Processing
**Purpose**: Handles real-time stock price feeds

**Stream Types**:
- `stock_ext`: Raw market data from Kafka (string format)
- `stock_w_ext`: Well-formed market data (JSONEachRow)
- `stock`: Mutable stream for latest prices

**Data Structure**:
```sql
SecurityID, Symbol, PreClosePx, LastPx, OpenPx, ClosePx, HighPx, LowPx
```

**Key Features**:
- **Primary Key**: `SecurityID` ensures latest price updates
- **Multiple Formats**: Supports both raw and structured data ingestion

### 4_etl.sql - Data Transformation Layer
**Purpose**: Transforms raw Kafka data into structured, queryable formats

**Materialized Views**:

**Order Processing**:
```sql
exchange_order_mv: Converts raw order JSON to typed columns
- Handles timestamp conversion
- Extracts order status, quantities, prices
- Maintains order lifecycle state
```

**Position Processing**:
```sql
position_mv: Tracks current holdings
- Real-time position updates
- Account-level aggregation
```

**Execution Processing**:
```sql
execution_mv: Processes trade executions
- Fee calculations
- P&L impact tracking
- Strategy attribution
```

**Stock Data Processing**:
```sql
stock_mv: Market data normalization
- Price type conversion
- Symbol standardization
```

### 5_pre.sql - Pre-Value Calculation
**Purpose**: Calculates baseline portfolio values for P&L computation

**Logic**:
```sql
prevalue = sum(LastPx * HoldingQty) for each (SecurityAccount, SecurityId)
```

**Usage**: This pre-value serves as the starting point for profit/loss calculations, representing the portfolio value at the beginning of the measurement period.

### 6_udf.sql - Advanced Analytics Functions
**Purpose**: Implements the **Continuous Auction Participation Rate** calculation

**Function: `part_rate`**

This is the most complex component, implementing the regulatory requirement for market maker participation monitoring.

**Algorithm Overview**:

1. **Order Book Management**:
   - Maintains separate buy and sell order maps
   - Validates order state transitions
   - Handles order updates and cancellations

2. **Price Level Calculation**:
   ```javascript
   // Find minimum buy price (highest valid buy order)
   sorted_buy.sort(price DESC)
   cumulative_amount += price * (qty - cum_qty)
   if (cumulative_amount > min_balance) min_buy_price = price
   
   // Find maximum sell price (lowest valid sell order)  
   sorted_sell.sort(price ASC)
   cumulative_amount += price * (qty - cum_qty)
   if (cumulative_amount > min_balance) max_sell_price = price
   ```

3. **Spread Calculation**:
   ```javascript
   spread = (max_sell_price - min_buy_price) * 2 / 
            max(max_sell_price + min_buy_price, 2)
   ```

4. **Participation Rate**:
   ```javascript
   if (spread <= min_spread) T += 1  // Valid sampling point
   
   // Market-specific time constants:
   SZ market: rate = T / 14220  // ~3.95 hours in seconds
   SH market: rate = T / 14400  // 4 hours in seconds
   ```

**Regulatory Compliance**: This calculation ensures market makers meet their obligation to maintain competitive bid-ask spreads during continuous auction periods.

### 7_metrics.sql - Real-Time Analytics Views
**Purpose**: Provides real-time trading metrics and P&L monitoring

**View: `rate_v` - Participation Rate Monitoring**:
```sql
-- Calculates participation rate every second using tumbling windows
SELECT SecurityId, StrategyId, window_start,
       part_rate(...) as rate,  -- Custom UDF from 6_udf.sql
       max(event_ts) as event_ts
FROM tumble(cte, 1s)  -- 1-second windows
GROUP BY SecurityId, StrategyId, window_start
```

**View: `profit_v` - Real-Time P&L Calculation**:
```sql
-- Real-time profit calculation with 2-second emission
profit = current_value + sell_amount - buy_amount - fees - pre_value

Components:
- buy_amount: Total purchase cost
- sell_amount: Total sale proceeds  
- deal_fee: Transaction costs
- cur_value: Current position value (HoldingQty * LastPx)
- pre_value: Starting portfolio value
```

**Key Features**:
- **Windowing**: Uses tumbling windows for consistent time-based aggregation
- **Real-time Updates**: 1-2 second latency for metric updates
- **Multi-dimensional**: Tracks by SecurityId, StrategyId, and SecurityAccount

## System Benefits

**Real-Time Monitoring**:
- Sub-second latency for critical trading metrics
- Continuous compliance monitoring
- Immediate P&L visibility

**Regulatory Compliance**:
- Automated participation rate calculation
- Audit trail for all trading activities
- Risk limit monitoring

**Operational Efficiency**:
- Streamlined data pipeline from raw events to business metrics
- Scalable architecture handling thousands of events per second
- Flexible configuration management

## Data Flow Summary

```
Kafka Topics → External Streams → Materialized Views → Analytics Views
     ↓              ↓                    ↓                ↓
Raw Events → Structured Data → Business Logic → Real-time Metrics
```

## Usage Examples

**Monitor Participation Rate**:
```sql
SELECT * FROM invest_insights.rate_v 
WHERE SecurityId = '600001' 
ORDER BY window_start DESC LIMIT 10;
```

**Track Real-time P&L**:
```sql
SELECT * FROM invest_insights.profit_v 
WHERE SecurityAccount = 'a123'
ORDER BY event_ts DESC;
```

**Performance Tuning**:
- Adjust `eps` parameters in generator streams for load testing
- Modify retention policies based on storage requirements
- Configure Kafka partitions for optimal throughput

This system provides a foundation for sophisticated trading operations with real-time analytics, regulatory compliance, and operational monitoring capabilities.