-- total survival time in last 7 games
CREATE MATERIALIZED VIEW game.mv_total_survival_last_7_game AS
select 
    user_id, 
    array_sum(x->x, group_array_last (event_data:survival_time::int, 7)) as total_survival_time_in_last_7_games
from game.player_actions
where event_type = 'match_end'
group by user_id;

-- total spend in last 10 transactions
CREATE MATERIALIZED VIEW game.mv_total_spend_last_10_transaction AS
select 
    user_id, 
    array_sum(x->x, group_array_last (amount_usd, 10)) as total_spend
from game.transactions
group by user_id

-- feature 5 lost in a row last 5 games
CREATE MATERIALIZED VIEW game.mv_5_lost_in_a_row AS
select 
    user_id, 
    group_array_last(event_data:result, 5) as last_five_game_result,
    case 
        when length(last_five_game_result) = 5 
             and array_count(x -> x = 'lost', last_five_game_result) = 5 
        then true 
        else false 
    end as all_five_lost
from game.player_actions
where event_type = 'match_end'
group by user_id
