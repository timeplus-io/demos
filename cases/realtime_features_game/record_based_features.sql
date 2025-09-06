-- total survival time in last 7 games
select 
    user_id, 
    array_sum(x->x, group_array_last (event_data:survival_time::int, 7)) as total_survival_time_in_last_7_games
from game.player_actions
where event_type = 'match_end'
group by user_id;


-- total spend in last 10 transactions
select 
    user_id, 
    array_sum(x->x, group_array_last (amount_usd, 10)) as total_spend
from game.transactions
group by user_id
settings seek_to = 'earliest'