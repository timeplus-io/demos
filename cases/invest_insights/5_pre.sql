-- prepare pre-value
insert into invest_insights.pre_value (SecurityAccount, SecurityId, prevalue)
select a.SecurityAccount, a.SecurityId, sum(b.LastPx * a.HoldingQty) as prevalue
from table(invest_insights.position) as a
join table(invest_insights.stock) as b
on a.SecurityId = b.SecurityID
group by SecurityAccount, SecurityId;