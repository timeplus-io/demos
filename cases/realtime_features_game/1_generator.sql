CREATE DATABASE IF NOT EXISTS game_source;
CREATE DATABASE IF NOT EXISTS game;

CREATE RANDOM STREAM game_source.player_actions
(
    -- User identification
    user_id string DEFAULT concat('usr_', to_string(rand64() % 20000)), -- Simulates a pool of 20k users
    
    -- Session tracking
    `session_id` string DEFAULT concat('sess_', to_string(rand64() % 1000)), -- 1k sessions
    
    -- ISO timestamp format
    `timestamp` string DEFAULT format_datetime(now64(), '%Y-%m-%dT%H:%i:%s.%fZ'),
    
    -- Event Categorization
    event_type enum8('match_start' = 1, 'item_pickup' = 2, 'player_elimination' = 3, 'match_end' = 4) DEFAULT multi_if(
        rand() % 100 < 10, 'match_start',         -- 10%
        rand() % 100 < 50, 'item_pickup',          -- 40%
        rand() % 100 < 90, 'player_elimination',   -- 40%
        'match_end'                              -- 10%
    ),
    game_mode enum8('battle_royale' = 1, 'team_deathmatch' = 2, 'capture_the_flag' = 3) DEFAULT multi_if(
        rand() % 100 < 75, 'battle_royale',      -- 75%
        rand() % 100 < 95, 'team_deathmatch',    -- 20%
        'capture_the_flag'                       -- 5%
    ),
    
    -- Match ID
    `match_id` string DEFAULT concat('match_', to_string(rand(3) % 900 + 100)), -- Like match_789

    -- Nested Event Data as a STRING
    -- The JSON structure is manually built using concat() for maximum control.
    event_data string DEFAULT concat(
        '{',
        '\"placement\":', to_string(multi_if(event_type = 'match_end', abs(to_int32(rand_normal(50, 25))) + 1, 0)), ',',
        '\"kills\":', to_string(multi_if(event_type IN ('player_elimination', 'match_end'), rand_poisson(2), 0)), ',',
        '\"damage_dealt\":', to_string(multi_if(event_type IN ('player_elimination', 'match_end'), to_int32(exp(rand_normal(6.5, 1.2))), 0)), ',',
        '\"survival_time\":', to_string(multi_if(event_type = 'match_end', to_int32(rand_uniform(300, 1200)), 0)), ',',
        '\"result\":"', to_string(multi_if(event_type = 'match_end',array_element(['win', 'loss'], (rand() % 2) + 1) , 'na')), '\",',
        '\"items_used\":[\"', array_element(['med_kit', 'shield_potion', 'grenade', 'smoke_bomb', 'bandages', ''], (rand() % 6) + 1), '\",\"', array_element(['med_kit', 'shield_potion', 'grenade', ''], (rand() % 4) + 1), '\"],',
        '\"location_final\":{',
            '\"x\":', to_string(round(rand_uniform(0, 1500), 2)), ',',
            '\"y\":', to_string(round(rand_uniform(0, 1500), 2)),
        '}',
        '}'
    ),

    -- Nested Device Info as a STRING
    -- Manually constructed JSON string.
    device_info string DEFAULT concat(
        '{',
        '\"platform\":\"', array_element(['mobile_ios', 'mobile_android', 'pc_windows', 'console_ps5', 'console_xbox'], (rand() % 5) + 1), '\",',
        '\"device_model\":\"', array_element(['iPhone_15_Pro', 'Samsung_S24', 'Gaming_PC_Rig', 'PlayStation_5', 'Xbox_Series_X'], (rand() % 5) + 1), '\",',
        '\"os_version\":\"', array_element(['iOS_17.1', 'Android_14', 'Windows_11', 'PS5_OS_9.0', 'Xbox_OS_10.0'], (rand() % 5) + 1), '\",',
        '\"app_version\":\"', array_element(['2.4.1', '2.4.0', '2.3.5', '2.2.0'], (rand() % 4) + 1), '\"',
        '}'
    )
)
SETTINGS eps = 50;


CREATE RANDOM STREAM game_source.transactions (
    -- Core Identifiers & Timestamp
    transaction_id string DEFAULT concat('txn_', to_string(rand64())),
    user_id string DEFAULT concat('usr_', to_string(rand64() % 20000)), -- Simulates a pool of 20k users
    session_id string DEFAULT concat('sess_', to_string(rand64() % 1000)), -- Simulates a pool of 1k sessions
    timestamp datetime64(3) DEFAULT now64(3),
    
    -- Transaction Details
    -- Weighted distribution: IAP is most common, followed by subscriptions, then rare refunds.
    transaction_type enum8('iap_purchase' = 1, 'subscription' = 2, 'refund' = 3) DEFAULT multi_if(
        rand() % 100 < 80, 'iap_purchase',       -- 80% chance
        rand() % 100 < 98, 'subscription',       -- 18% chance
        'refund'                                 -- 2% chance
    ),

    -- An even distribution of common item categories.
    item_category enum8('cosmetic' = 1, 'power_up' = 2, 'loot_box' = 3, 'battle_pass' = 4) DEFAULT array_element(
        ['cosmetic', 'power_up', 'loot_box', 'battle_pass'], 
        (rand() % 4) + 1
    ),

    -- Generates a plausible, structured item ID.
    item_id string DEFAULT concat(
        array_element(['skin', 'emote', 'booster', 'pack'], (rand() % 4) + 1),
        '_',
        array_element(['common', 'rare', 'epic', 'legendary'], (rand() % 4) + 1),
        '_',
        array_element(['dragon', 'phoenix', 'reaver', 'starfall'], (rand() % 4) + 1)
    ),

    -- Financial Information
    -- Models fixed pricing tiers, which is more realistic than a random float.
    amount_usd float64 DEFAULT array_element([0.99, 4.99, 9.99, 19.99, 49.99, 99.99], (rand() % 6) + 1),

    currency_type enum8('real_money' = 1, 'virtual_currency' = 2) DEFAULT multi_if(
        rand() % 100 < 90, 'real_money',        -- 90% chance
        'virtual_currency'                      -- 10% chance
    ),

    payment_method enum8('apple_pay' = 1, 'google_pay' = 2, 'credit_card' = 3, 'paypal' = 4) DEFAULT array_element(
        ['apple_pay', 'google_pay', 'credit_card', 'paypal'], 
        (rand() % 4) + 1
    ),

    -- Location Data (Generated as a JSON String)
    -- This string is manually constructed to ensure a valid JSON format.
    location string DEFAULT concat(
        '{',
        '\"country\":\"', array_element(['US', 'CA', 'GB', 'DE', 'JP', 'AU'], (rand() % 6) + 1), '\",',
        '\"region\":\"', array_element(['California', 'Texas', 'New York', 'Florida', 'Ontario', 'Quebec', 'England', 'Scotland', 'Bavaria', 'Tokyo', 'New South Wales'], (rand() % 11) + 1), '\",',
        '\"city\":\"', array_element(['Los Angeles', 'Houston', 'New York City', 'Miami', 'Toronto', 'Montreal', 'London', 'Edinburgh', 'Munich', 'Tokyo', 'Sydney'], (rand() % 11) + 1), '\",',
        '\"latitude\":', to_string(round(rand_uniform(30.0, 50.0), 4)), ',',
        '\"longitude\":', to_string(round(rand_uniform(-125.0, -70.0), 4)),
        '}'
    ),

    -- A unique hex string to simulate a device fingerprint.
    device_fingerprint string DEFAULT concat('fp_', lower(hex(rand64())))
) SETTINGS eps = 10;


CREATE RANDOM STREAM game_source.social_events (
    -- Core Identifiers & Timestamp
    -- The user initiating the interaction from a pool of 20,000 users.
    user_id string DEFAULT concat('usr_', to_string(rand64() % 20000)), -- Simulates a pool of 20k users
    
    -- The user receiving the interaction.
    target_user_id string DEFAULT concat('usr_', to_string(rand() % 20000)),

    timestamp datetime64(3) DEFAULT now64(3),

    -- Interaction Details
    -- Weighted distribution: messages and likes are common, reports are rare.
    interaction_type enum8('message' = 1, 'like' = 2, 'friend_request' = 3, 'party_invite' = 4, 'report' = 5) DEFAULT multi_if(
        rand() % 100 < 50, 'message',            -- 50%
        rand() % 100 < 75, 'like',              -- 25%
        rand() % 100 < 90, 'friend_request',    -- 15%
        rand() % 100 < 98, 'party_invite',      -- 8%
        'report'                                -- 2%
    ),

    -- The context where the interaction takes place (e.g., in a lobby, after a match).
    context enum8('post_match' = 1, 'in_lobby' = 2, 'profile_view' = 3, 'global_chat' = 4) DEFAULT array_element(
        ['post_match', 'in_lobby', 'profile_view', 'global_chat'], 
        (rand() % 4) + 1
    ),

    -- Context-Aware Additional Data (as a JSON String)
    -- The content of this field changes based on the interaction_type and context.
    additional_data string DEFAULT multi_if(
        -- Scenario 1: Post-match interactions
        context = 'post_match' AND interaction_type IN ('friend_request', 'party_invite'),
        concat(
            '{\"match_id\":\"match_', to_string(rand() % 50000), 
            '\",\"message\":\"', array_element(['Good game!', 'GG wanna team up?', 'Nice plays!'], (rand() % 3) + 1), '\"}'
        ),
        -- Scenario 2: Reports, which always include a reason
        interaction_type = 'report',
        concat(
            '{\"match_id\":\"match_', to_string(rand() % 50000), 
            '\",\"reason\":\"', array_element(['cheating', 'toxic_behavior', 'spam'], (rand() % 3) + 1), '\"}'
        ),
        -- Scenario 3: Simple messages in chat
        interaction_type = 'message',
        concat(
            '{\"message\":\"', array_element(['lol', 'anyone for a match?', 'LFG', 'hey'], (rand() % 4) + 1), '\"}'
        ),
        -- Default: An empty JSON object for other cases like 'like'
        '{}'
    )
) SETTINGS eps = 25;

CREATE RANDOM STREAM game_source.performance_metrics (
    user_id string DEFAULT concat('usr_', to_string(rand64() % 20000)), -- Simulates a pool of 20k users
    session_id string DEFAULT concat('sess_', to_string(rand64() % 1000)), -- Simulates a pool of 1k sessions
    timestamp datetime64(3) DEFAULT now64(3),

    -- Device stats as JSON
    device_stats string DEFAULT concat(
        '{',
          '"fps_avg":', to_string(round(rand_normal(60, 10), 1)), ',',
          '"fps_min":', to_string(round(rand_uniform(20, 60), 0)), ',',
          '"memory_usage_mb":', to_string(round(rand_normal(2048, 512), 0)), ',',
          '"battery_level":', to_string((rand() % 100)), ',',
          '"network_latency_ms":', to_string(round(rand_normal(50, 15), 0)), ',',
          '"packet_loss_pct":', to_string(round(rand_uniform(0.0, 2.0), 2)),
        '}'
    ),

    -- Game stats as JSON
    game_stats string DEFAULT concat(
        '{',
          '"load_time_ms":', to_string(round(rand_normal(3000, 500), 0)), ',',
          '"crash_occurred":', multi_if((rand() % 100) < 5, 'true', 'false'), ',', -- ~5% crashes
          '"error_count":', to_string(rand() % 5),
        '}'
    )
) SETTINGS eps = 20;


-- target streams

CREATE STREAM game.player_actions
(
    user_id string, 
    session_id string,
    timestamp string,
    event_type enum8('match_start' = 1, 'item_pickup' = 2, 'player_elimination' = 3, 'match_end' = 4),
    game_mode enum8('battle_royale' = 1, 'team_deathmatch' = 2, 'capture_the_flag' = 3),
    match_id string,
    event_data string,
    device_info string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

CREATE STREAM game.transactions (
    transaction_id string,
    user_id string,
    session_id string,
    timestamp datetime64(3),
    transaction_type enum8('iap_purchase' = 1, 'subscription' = 2, 'refund' = 3),
    item_category enum8('cosmetic' = 1, 'power_up' = 2, 'loot_box' = 3, 'battle_pass' = 4),
    item_id string,
    amount_usd float64,
    currency_type enum8('real_money' = 1, 'virtual_currency' = 2),
    payment_method enum8('apple_pay' = 1, 'google_pay' = 2, 'credit_card' = 3, 'paypal' = 4),
    location string,
    device_fingerprint string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

CREATE STREAM game.social_events (
    user_id string,
    target_user_id string,
    timestamp datetime64(3),
    interaction_type enum8('message' = 1, 'like' = 2, 'friend_request' = 3, 'party_invite' = 4, 'report' = 5),
    context enum8('post_match' = 1, 'in_lobby' = 2, 'profile_view' = 3, 'global_chat' = 4),
    additional_data string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

CREATE STREAM game.performance_metrics (
    user_id string,
    session_id string,
    timestamp datetime64(3),
    device_stats string,
    game_stats string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';

-- simulate data ingestions
CREATE MATERIALIZED VIEW game_source.player_actions_mv
INTO game.player_actions
AS
    SELECT * FROM game_source.player_actions;

CREATE MATERIALIZED VIEW game_source.transactions_mv
INTO game.transactions
AS
    SELECT * FROM game_source.transactions;

CREATE MATERIALIZED VIEW game_source.social_events_mv
INTO game.social_events
AS
    SELECT * FROM game_source.social_events;

CREATE MATERIALIZED VIEW game_source.performance_metrics_mv
INTO game.performance_metrics
AS
    SELECT * FROM game_source.performance_metrics;


