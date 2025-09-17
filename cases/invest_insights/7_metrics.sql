
create view invest_insights.rate_v as
with cte as
    (select
        SecurityId, StrategyId, OrderId, extract(Symbol, '.*\.(.*)') as market, Side, Quantity, CumQuantity, Price, OrdStatus, event_ts, b.minReportBalance, b.minSpread, a._tp_time
    From
        invest_insights.exchange_order as a
    join invest_insights.cfg as b
    on a.SecurityId = b.securityId)
select
    SecurityId, StrategyId, window_start,
    part_rate(OrderId, market, Side, Quantity, CumQuantity, Price, OrdStatus, minReportBalance, minSpread) as rate,
    max(event_ts) as event_ts,
    now64(6) as ts
From
tumble(cte, 1s)
GROUP BY SecurityId, StrategyId, window_start;

create view invest_insights.profit_v as
select
    sum_if(px * qty, side = '1') as buy_amount,
    sum_if(px * qty, side = '2') as sell_amount,
    sum(fee) as deal_fee,
    latest(HoldingQty*f.LastPx) as cur_value,
    latest(pre_value) as pre_value,
    latest(event_ts) as event_ts,
    (cur_value + sell_amount - buy_amount - deal_fee  - pre_value) as profit,
    now64(3) as ts,
    SecurityId, SecurityAccount
from
    (
        select event_ts, px, qty, fee, side, c.SecurityId, c.SecurityAccount, pre_value,
        d.HoldingQty as HoldingQty
        from
        (
          select
            a.event_ts, a.LastPx as px, a.LastQty as qty, a.Fee as fee, a.EntrustDirection as side, a.SecurityId as SecurityId, a.SecurityAccount as SecurityAccount,
            b.prevalue as pre_value
          from invest_insights.execution as a
          join table(invest_insights.pre_value) as b
          on a.SecurityId = b.SecurityId and a.SecurityAccount = b.SecurityAccount
        ) as c
        join invest_insights.position as d
        on c.SecurityId = d.SecurityId and c.SecurityAccount = d.SecurityAccount
    ) as e
join invest_insights.stock as f
on e.SecurityId = f.SecurityID
group by SecurityId, SecurityAccount emit periodic 2s