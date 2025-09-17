create materialized view if not exists invest_insights.exchange_order_mv into invest_insights.exchange_order as
select
    to_uint64_or_default(raw:`event_ts`, 0::uint64)  as event_ts,
    to_date(raw:`TradeDate`) as TradeDate,
    raw:OrderId as OrderId,
    raw:SecurityExchange as SecurityExchange,
    raw:SecurityAccount as SecurityAccount,
    raw:SecurityId as SecurityId,
    raw:Symbol as Symbol,
    raw:Side as Side,
    to_float64_or_default(raw:Quantity, 0.0) as Quantity,
    to_float64_or_default(raw:Price, 0.0) as Price,
    to_float64_or_default(raw:CumQuantity, 0.0) as CumQuantity,
    raw:OrdStatus as OrdStatus,
    raw:StrategyId as StrategyId,
    _tp_time
from invest_insights.exchange_order_ext;

create materialized view if not exists invest_insights.position_mv into invest_insights.position as
select
    to_uint64_or_default(raw:`event_ts`, 0::uint64)  as event_ts,
    to_date(raw:`TradeDate`) as TradeDate,
    raw:SecurityAccount as SecurityAccount,
    raw:SecurityId as SecurityId,
    to_float64_or_default(raw:HoldingQty, 0.0) as HoldingQty,
    _tp_time
from invest_insights.position_ext;

create materialized view if not exists invest_insights.execution_mv into invest_insights.execution as
select
    to_uint64_or_default(raw:`event_ts`, 0::uint64)  as event_ts,
    raw:OrderId as OrderId,
    to_date(raw:`TradeDate`) as TradeDate,
    raw:SecurityAccount as SecurityAccount,
    raw:SecurityId as SecurityId,
    raw:EntrustDirection as EntrustDirection,
    to_float64_or_default(raw:LastQty, 0.0) as LastQty,
    to_float64_or_default(raw:LastPx, 0.0) as LastPx,
    to_float64_or_default(raw:Fee, 0.0) as Fee,
    raw:StrategyId as StrategyId,
    _tp_time
from invest_insights.execution_ext;

create materialized view if not exists invest_insights.stock_mv into invest_insights.stock as
select
    to_uint64_or_default(raw:`event_ts`, 0::uint64)  as event_ts,
    raw:SecurityID as SecurityID,
    raw:Symbol as Symbol,
    to_float64_or_default(raw:PreClosePx, 0.0) as PreClosePx,
    to_float64_or_default(raw:LastPx, 0.0) as LastPx,
    to_float64_or_default(raw:OpenPx, 0.0) as OpenPx,
    to_float64_or_default(raw:ClosePx, 0.0) as ClosePx,
    to_float64_or_default(raw:HighPx, 0.0) as HighPx,
    to_float64_or_default(raw:LowPx, 0.0) as LowPx,
    _tp_time
from invest_insights.stock_ext;
