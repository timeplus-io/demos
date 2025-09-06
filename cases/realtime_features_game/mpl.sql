CREATE DATABASE IF NOT EXISTS game;

CREATE RANDOM STREAM game.user_won (
    -- Game ID
    game_id string DEFAULT to_string(rand() % 9999 + 1),
    
    -- Cash Entry Fee
    cash_entry_fee string DEFAULT to_string(multi_if(
        (rand() % 100) <= 30, rand() % 20 + 5,
        (rand() % 100) <= 70, rand() % 50 + 25,
        rand() % 200 + 75
    )),
    
    -- User ID
    user_id string DEFAULT to_string(rand() % 99999 + 1000),
    
    -- \tEntry Currency
    tab_entry_currency string DEFAULT 'CASH',
    
    -- Rounds
    rounds string DEFAULT to_string(rand() % 10),
    
    -- Bonus Coin Available
    bonus_coin_available bool DEFAULT rand_bernoulli(0.3),
    
    -- Opponent Entry Fee
    opponent_entry_fee string DEFAULT multi_if(
        (rand() % 100) <= 80, '0.0',
        to_string(round(rand_uniform(1.0, 5.0), 1))
    ),
    
    -- Deck Count
    deck_count string DEFAULT to_string(rand() % 3 + 1),
    
    -- Hof Lobby Limit
    hof_lobby_limit string DEFAULT multi_if(
        (rand() % 100) <= 90, '0.0',
        to_string(round(rand_uniform(100.0, 1000.0), 1))
    ),
    
    -- Player RTP
    player_rtp string DEFAULT multi_if(
        (rand() % 100) <= 70, '-1.0',
        to_string(round(rand_uniform(85.0, 105.0), 1))
    ),
    
    -- Active Players
    active_players string DEFAULT to_string(rand() % 8),
    
    -- Tournament ID
    tournament_id string DEFAULT to_string(rand() % 999999 + 100000),
    
    -- Bonus Deducted
    bonus_deducted string DEFAULT '0.0',
    
    -- Bumper Extra Winnings
    bumper_extra_winnings string DEFAULT multi_if(
        (rand() % 100) <= 95, '0.0',
        to_string(round(rand_uniform(1.0, 20.0), 1))
    ),
    
    -- Max Players
    max_players string DEFAULT multi_if(
        (rand() % 100) <= 60, '2',
        (rand() % 100) <= 80, '4',
        to_string(rand() % 6 + 6)
    ),
    
    -- Start Date
    start_date datetime64(3) DEFAULT now64(),
    
    -- Rake Rate Mutant
    rake_rate_mutant string DEFAULT array_element(['5.0', '8.0', '10.0', '12.0', '15.0'], (rand() % 5) + 1),
    
    -- Tournament Type
    tournament_type enum8('table' = 1, 'knockout' = 2, 'league' = 3) DEFAULT array_element(['table', 'knockout', 'league'], (rand() % 3) + 1),
    
    -- Can Play Again
    can_play_again bool DEFAULT rand_bernoulli(0.8),
    
    -- won
    won bool DEFAULT rand_bernoulli(0.5),
    
    -- RTP Intervention
    rtp_intervention bool DEFAULT rand_bernoulli(0.1),
    
    -- \tTournament Name
    tab_tournament_name string DEFAULT array_element([
        'QA Sanity with Extra Config keys', 
        'Daily Championship', 
        'Weekend Warriors', 
        'Pro League Tournament',
        'Mega Contest Special'
    ], (rand() % 5) + 1),
    
    -- Hof Design Variant
    hof_design_variant enum8('V1' = 1, 'V2' = 2, 'V3' = 3) DEFAULT 'V1',
    
    -- CrossCountryEnabled
    cross_country_enabled bool DEFAULT rand_bernoulli(0.2),
    
    -- Hof Final Rank
    hof_final_rank string DEFAULT to_string(rand() % 100),
    
    -- Is Async Battle
    is_async_battle bool DEFAULT rand_bernoulli(0.3),
    
    -- Game Format
    game_format enum8('BATTLE_V1' = 1, 'BATTLE_V2' = 2, 'CLASSIC' = 3) DEFAULT array_element(['BATTLE_V1', 'BATTLE_V2', 'CLASSIC'], (rand() % 3) + 1),
    
    -- rtpbasedBumperReward
    rtp_based_bumper_reward bool DEFAULT rand_bernoulli(0.15),
    
    -- Variant
    variant string DEFAULT array_element(['NA', 'V1', 'V2', 'PREMIUM'], (rand() % 4) + 1),
    
    -- Prize Amount
    prize_amount string DEFAULT to_string(round(rand_uniform(2.0, 50.0), 1)),
    
    -- Is Hof Won
    is_hof_won bool DEFAULT rand_bernoulli(0.05),
    
    -- Currency ID
    currency_id enum8('INR' = 1, 'USD' = 2, 'EUR' = 3, 'BRL' = 4) DEFAULT array_element(['INR', 'USD', 'EUR', 'BRL'], (rand() % 4) + 1),
    
    -- Bonus Cash Entry Fee Mutant
    bonus_cash_entry_fee_mutant string DEFAULT '0.0',
    
    -- POD_NAME
    pod_name enum8('CARD-GAMES' = 1, 'SPORTS-GAMES' = 2, 'PUZZLE-GAMES' = 3) DEFAULT array_element(['CARD-GAMES', 'SPORTS-GAMES', 'PUZZLE-GAMES'], (rand() % 3) + 1),
    
    -- Bonus Coin Used
    bonus_coin_used string DEFAULT multi_if(
        (rand() % 100) <= 90, '0.0',
        to_string(round(rand_uniform(1.0, 20.0), 1))
    ),
    
    -- OptinRake
    optin_rake string DEFAULT '0.0',
    
    -- Country
    country string DEFAULT array_element(['[IN]', '[US]', '[BR]', '[GB]', '[CA]'], (rand() % 5) + 1),
    
    -- is Hof Default Score List
    is_hof_default_score_list bool DEFAULT rand_bernoulli(0.1),
    
    -- Level
    level string DEFAULT to_string(rand() % 100),
    
    -- Rounds Count
    rounds_count string DEFAULT to_string(rand() % 15),
    
    -- HoF Target Score List Count
    hof_target_score_list_count string DEFAULT to_string(rand() % 5 + 1),
    
    -- Hof User BTS Multiplier
    hof_user_bts_multiplier string DEFAULT to_string(round(rand_uniform(0.0, 3.0), 1)),
    
    -- Hof User Predicted Score
    hof_user_predicted_score string DEFAULT to_string(round(rand_uniform(0.0, 1000.0), 1)),
    
    -- RTP Calculated Extra Winnings
    rtp_calculated_extra_winnings string DEFAULT multi_if(
        (rand() % 100) <= 90, '0.0',
        to_string(round(rand_uniform(0.5, 10.0), 1))
    ),
    
    -- Hof Amount Credited
    hof_amount_credited string DEFAULT multi_if(
        (rand() % 100) <= 95, '0.0',
        to_string(round(rand_uniform(10.0, 100.0), 1))
    ),
    
    -- \tTournament Type
    tab_tournament_type string DEFAULT array_element(['1V1', '2V2', 'MULTI'], (rand() % 3) + 1),
    
    -- Is Hof Battle
    is_hof_battle bool DEFAULT rand_bernoulli(0.08),
    
    -- Tournament App Types
    tournament_app_types string DEFAULT array_element([
        '["CASH","PLAY_STORE","IOS"]',
        '["CASH","ANDROID"]',
        '["FREEMIUM","WEB"]'
    ], (rand() % 3) + 1),
    
    -- Is Won
    is_won bool DEFAULT rand_bernoulli(0.5),
    
    -- Point Value
    point_value string DEFAULT to_string(round(rand_uniform(0.0, 10.0), 1)),
    
    -- Total Players
    total_players string DEFAULT to_string(rand() % 12),
    
    -- Bonus Coin Balance
    bonus_coin_balance string DEFAULT to_string(round(rand_uniform(0.0, 100.0), 1)),
    
    -- Entry Fee
    entry_fee string DEFAULT to_string(round(rand_uniform(1.0, 10.0), 1)),
    
    -- Opponent User ID
    opponent_user_id string DEFAULT concat('[', to_string(rand() % 99999 + 1000), ']'),
    
    -- Rebuy
    rebuy string DEFAULT multi_if(
        (rand() % 100) <= 80, '0.0',
        to_string(rand() % 3 + 1)
    ),
    
    -- Tournament Name
    tournament_name string DEFAULT array_element([
        'QA Sanity with Extra Config keys', 
        'Daily Championship', 
        'Weekend Warriors', 
        'Pro League Tournament',
        'Mega Contest Special'
    ], (rand() % 5) + 1),
    
    -- App Type
    app_type enum8('Cash' = 1, 'Freemium' = 2, 'Premium' = 3) DEFAULT array_element(['Cash', 'Freemium', 'Premium'], (rand() % 3) + 1),
    
    -- Entry Currency
    entry_currency enum8('CASH' = 1, 'BONUS' = 2) DEFAULT 'CASH',
    
    -- Game Session ID
    game_session_id string DEFAULT concat('BIN-', lower(hex(uuid()))),
    
    -- Game Name
    game_name string DEFAULT array_element(['Bingo Skill', 'Poker Pro', 'Rummy Master', 'Teen Patti Gold', 'Fantasy Cricket'], (rand() % 5) + 1),
    
    -- Rake Rate
    rake_rate string DEFAULT array_element(['5.0', '8.0', '10.0', '12.0', '15.0'], (rand() % 5) + 1),
    
    -- Unique ID
    unique_id string DEFAULT concat('BIN-', lower(hex(uuid()))),
    
    -- Wins Count
    wins_count string DEFAULT to_string(rand() % 50),
    
    -- Win Amount
    win_amount string DEFAULT to_string(round(rand_uniform(3.0, 60.0), 1)),
    
    -- \tGame Config Name
    tab_game_config_name string DEFAULT array_element(['Bingo Skill', 'Poker Pro', 'Rummy Master', 'Teen Patti Gold', 'Fantasy Cricket'], (rand() % 5) + 1),
    
    -- Bonus Cash Entry Fee
    bonus_cash_entry_fee string DEFAULT '0.0',
    
    -- Hof User Final Score
    hof_user_final_score string DEFAULT to_string(round(rand_uniform(0.0, 2000.0), 1)),
    
    -- crossLobby
    cross_lobby bool DEFAULT rand_bernoulli(0.2),
    
    -- Is Cross Lobby
    is_cross_lobby bool DEFAULT rand_bernoulli(0.2),
    
    -- Loss Amount
    loss_amount string DEFAULT to_string(round(rand_uniform(1.0, 15.0), 1)),
    
    -- Cross Country Type
    cross_country_type enum8('DOMESTIC_INDIA' = 1, 'INTERNATIONAL' = 2) DEFAULT array_element(['DOMESTIC_INDIA', 'INTERNATIONAL'], (rand() % 2) + 1),
    
    -- MPL User ID
    mpl_user_id string DEFAULT multi_if(
        (rand() % 100) <= 80, '0.0',
        to_string(rand() % 999999 + 100000)
    ),
    
    -- Injected Country Code
    injected_country_code string DEFAULT array_element(['IN', 'US', 'BR', 'GB', 'CA'], (rand() % 5) + 1),
    
    -- Injected Profile Tier
    injected_profile_tier string DEFAULT array_element(['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Steel', 'Ruby', 'Emerald'], (rand() % 8) + 1),
    
    -- Injected App Type
    injected_app_type enum8('ANDROID' = 1, 'IOS' = 2, 'WEB' = 3) DEFAULT array_element(['ANDROID', 'IOS', 'WEB'], (rand() % 3) + 1),
    
    -- Mobile Number
    mobile_number string DEFAULT concat('+', to_string(rand() % 9000000000 + 1000000000)),
    
    -- Injected Language Code
    injected_language_code enum8('en_IN' = 1, 'hi_IN' = 2, 'en_US' = 3, 'pt_BR' = 4) DEFAULT array_element(['en_IN', 'hi_IN', 'en_US', 'pt_BR'], (rand() % 4) + 1)
    
) SETTINGS eps = 90;


CREATE RANDOM STREAM game.user_played_game (
    -- User ID
    user_id string DEFAULT to_string(rand() % 99999 + 10000),
    
    -- Game End Tech Reason
    game_end_tech_reason enum8(
        'DROP' = 1, 
        'TIMEOUT' = 2, 
        'COMPLETE' = 3, 
        'DISCONNECT' = 4, 
        'FORFEIT' = 5,
        'ERROR' = 6
    ) DEFAULT array_element([
        'DROP', 'TIMEOUT', 'COMPLETE', 'DISCONNECT', 'FORFEIT', 'ERROR'
    ], multi_if(
        (rand() % 100) <= 40, 1,    -- 40% DROP
        (rand() % 100) <= 60, 3,    -- 20% COMPLETE  
        (rand() % 100) <= 75, 2,    -- 15% TIMEOUT
        (rand() % 100) <= 85, 4,    -- 10% DISCONNECT
        (rand() % 100) <= 95, 5,    -- 10% FORFEIT
        6                           -- 5% ERROR
    )),
    
    -- newHistoryEventsPlayer
    new_history_events_player bool DEFAULT rand_bernoulli(0.7),
    
    -- Format
    format string DEFAULT to_string(rand() % 5 + 1),
    
    -- Timeout turns missed
    timeout_turns_missed string DEFAULT multi_if(
        (rand() % 100) <= 70, '0.0',
        (rand() % 100) <= 90, to_string(rand() % 3 + 1),
        to_string(rand() % 8 + 4)
    ),
    
    -- Game ID
    game_id string DEFAULT concat(to_string(rand() % 9999999 + 1000000), '.0'),
    
    -- Bonus Time Duration
    bonus_time_duration string DEFAULT multi_if(
        (rand() % 100) <= 40, '15.0',
        (rand() % 100) <= 65, '30.0',
        (rand() % 100) <= 80, '10.0',
        (rand() % 100) <= 90, '60.0',
        '0.0'
    ),
    
    -- Battle ID
    battle_id string DEFAULT concat(
        lower(random_printable_ascii(32)), 
        'cidone_', 
        to_string(rand() % 999 + 100)
    ),
    
    -- Boot
    boot float32 DEFAULT multi_if(
        (rand() % 100) <= 50, 0.5,
        (rand() % 100) <= 70, round(rand_uniform(0.1, 0.4), 1),
        (rand() % 100) <= 85, round(rand_uniform(0.6, 0.9), 1),
        round(rand_uniform(1.0, 2.0), 1)
    ),
    
    -- Timeout Duration
    timeout_duration string DEFAULT multi_if(
        (rand() % 100) <= 30, '5.0',
        (rand() % 100) <= 60, '12.0',
        (rand() % 100) <= 85, '30.0',
        to_string(round(rand_uniform(45.0, 120.0), 1))
    ),
    
    -- Injected Country Code
    injected_country_code enum8(
        'IN' = 1, 'US' = 2, 'BR' = 3, 'GB' = 4, 'CA' = 5, 
        'AU' = 6, 'DE' = 7, 'FR' = 8, 'SG' = 9, 'UAE' = 10
    ) DEFAULT array_element([
        'IN', 'US', 'BR', 'GB', 'CA', 'AU', 'DE', 'FR', 'SG', 'UAE'
    ], multi_if(
        (rand() % 100) <= 60, 1,
        (rand() % 100) <= 75, 2,
        (rand() % 100) <= 85, 3,
        (rand() % 7) + 4
    )),
    
    -- Injected Profile Tier
    injected_profile_tier enum8(
        'Bronze' = 1, 'Silver' = 2, 'Gold' = 3, 'Platinum' = 4, 
        'Diamond' = 5, 'Topaz' = 6, 'Ruby' = 7, 'Emerald' = 8
    ) DEFAULT array_element([
        'Bronze', 'Silver', 'Gold', 'Platinum', 
        'Diamond', 'Topaz', 'Ruby', 'Emerald'
    ], multi_if(
        (rand() % 100) <= 25, 1,
        (rand() % 100) <= 45, 2,
        (rand() % 100) <= 60, 3,
        (rand() % 100) <= 73, 4,
        (rand() % 100) <= 83, 5,
        (rand() % 100) <= 90, 6,
        (rand() % 100) <= 96, 7,
        8
    )),
    
    -- Injected App Type
    injected_app_type enum8(
        'CASH' = 1, 'ANDROID' = 2, 'IOS' = 3, 
        'WEB' = 4, 'TABLET' = 5
    ) DEFAULT array_element([
        'CASH', 'ANDROID', 'IOS', 'WEB', 'TABLET'
    ], multi_if(
        (rand() % 100) <= 45, 1,
        (rand() % 100) <= 70, 2,
        (rand() % 100) <= 88, 3,
        (rand() % 100) <= 96, 4,
        5
    )),
    
    -- Mobile Number
    mobile_number string DEFAULT concat('+', 
        to_string(rand() % 900000000000 + 100000000000)
    ),
    
    -- Injected Language Code
    injected_language_code enum8(
        'en_IN' = 1, 'hi_IN' = 2, 'en_US' = 3, 'pt_BR' = 4,
        'en_GB' = 5, 'fr_FR' = 6, 'de_DE' = 7, 'zh_CN' = 8,
        'es_ES' = 9, 'ar_AE' = 10
    ) DEFAULT array_element([
        'en_IN', 'hi_IN', 'en_US', 'pt_BR', 'en_GB',
        'fr_FR', 'de_DE', 'zh_CN', 'es_ES', 'ar_AE'
    ], multi_if(
        (rand() % 100) <= 50, 1,
        (rand() % 100) <= 65, 2,
        (rand() % 100) <= 75, 3,
        (rand() % 100) <= 83, 4,
        (rand() % 6) + 5
    ))
    
) SETTINGS eps = 100;


CREATE RANDOM STREAM game.user_journey_activated (
    -- entryFee
    entry_fee string DEFAULT multi_if(
        (rand() % 100) <= 50, '1.0',
        (rand() % 100) <= 75, '5.0',
        (rand() % 100) <= 90, '10.0',
        to_string(round(rand_uniform(2.0, 50.0), 1))
    ),
    
    -- resurrected
    resurrected bool DEFAULT rand_bernoulli(0.15),
    
    -- journeyId
    journey_id string DEFAULT concat(
        random_printable_ascii(12), 
        '_', 
        lower(hex(uuid()))
    ),
    
    -- journeyGameId
    journey_game_id string DEFAULT to_string(rand() % 9999 + 1000),
    
    -- discountingEndTime
    discounting_end_time string DEFAULT multi_if(
        (rand() % 100) <= 80, '0.0',
        to_string(rand() % 86400000 + 1755586575732)
    ),
    
    -- gameplayLimitEnabled
    gameplay_limit_enabled bool DEFAULT rand_bernoulli(0.3),
    
    -- discountingApplied
    discounting_applied bool DEFAULT rand_bernoulli(0.2),
    
    -- gameplayCount
    gameplay_count string DEFAULT multi_if(
        (rand() % 100) <= 60, '0',
        (rand() % 100) <= 85, to_string(rand() % 5 + 1),
        to_string(rand() % 20 + 5)
    ),
    
    -- triggerId
    trigger_id string DEFAULT multi_if(
        (rand() % 100) <= 70, '',
        concat('trigger_', to_string(rand() % 999999 + 100000))
    ),
    
    -- userId
    user_id string DEFAULT to_string(rand() % 999999999 + 100000000),
    
    -- benefit
    benefit enum8(
        'LOBBY' = 1, 'CASHBACK' = 2, 'BONUS' = 3, 
        'DISCOUNT' = 4, 'FREEPLAY' = 5, 'MULTIPLIER' = 6
    ) DEFAULT array_element([
        'LOBBY', 'CASHBACK', 'BONUS', 'DISCOUNT', 'FREEPLAY', 'MULTIPLIER'
    ], multi_if(
        (rand() % 100) <= 40, 1,  -- 40% LOBBY
        (rand() % 100) <= 60, 2,  -- 20% CASHBACK
        (rand() % 100) <= 75, 3,  -- 15% BONUS
        (rand() % 100) <= 85, 4,  -- 10% DISCOUNT
        (rand() % 100) <= 95, 5,  -- 10% FREEPLAY
        6                         -- 5% MULTIPLIER
    )),
    
    -- segmentName
    segment_name string DEFAULT multi_if(
        (rand() % 100) <= 60, '',
        array_element([
            'high_value', 'new_user', 'returning_user', 
            'vip_player', 'casual_gamer'
        ], (rand() % 5) + 1)
    ),
    
    -- gameId3
    game_id3 string DEFAULT multi_if(
        (rand() % 100) <= 90, '0',
        to_string(rand() % 9999 + 1000)
    ),
    
    -- gameId4
    game_id4 string DEFAULT multi_if(
        (rand() % 100) <= 90, '0',
        to_string(rand() % 9999 + 1000)
    ),
    
    -- gameId1
    game_id1 string DEFAULT multi_if(
        (rand() % 100) <= 90, '0',
        to_string(rand() % 9999 + 1000)
    ),
    
    -- gameId2
    game_id2 string DEFAULT multi_if(
        (rand() % 100) <= 90, '0',
        to_string(rand() % 9999 + 1000)
    ),
    
    -- endTime
    end_time string DEFAULT to_string(rand() % 86400000 + 1755586575732),
    
    -- bonusCashAmount
    bonus_cash_amount string DEFAULT multi_if(
        (rand() % 100) <= 80, '0',
        to_string(rand() % 100 + 10)
    ),
    
    -- actionName
    action_name enum8(
        'Lobby_Surfacing' = 1, 'Game_Entry' = 2, 'Tournament_Join' = 3,
        'Daily_Bonus' = 4, 'Level_Up' = 5, 'Achievement_Unlock' = 6,
        'Referral_Complete' = 7, 'First_Deposit' = 8
    ) DEFAULT array_element([
        'Lobby_Surfacing', 'Game_Entry', 'Tournament_Join',
        'Daily_Bonus', 'Level_Up', 'Achievement_Unlock',
        'Referral_Complete', 'First_Deposit'
    ], multi_if(
        (rand() % 100) <= 35, 1,  -- 35% Lobby_Surfacing
        (rand() % 100) <= 55, 2,  -- 20% Game_Entry
        (rand() % 100) <= 70, 3,  -- 15% Tournament_Join
        (rand() % 100) <= 80, 4,  -- 10% Daily_Bonus
        (rand() % 100) <= 87, 5,  -- 7% Level_Up
        (rand() % 100) <= 93, 6,  -- 6% Achievement_Unlock
        (rand() % 100) <= 97, 7,  -- 4% Referral_Complete
        8                         -- 3% First_Deposit
    )),
    
    -- Injected Country Code
    injected_country_code enum8(
        'IN' = 1, 'US' = 2, 'BR' = 3, 'GB' = 4, 'CA' = 5,
        'AU' = 6, 'DE' = 7, 'FR' = 8, 'SG' = 9, 'UAE' = 10
    ) DEFAULT array_element([
        'IN', 'US', 'BR', 'GB', 'CA', 'AU', 'DE', 'FR', 'SG', 'UAE'
    ], multi_if(
        (rand() % 100) <= 65, 1,  -- 65% India
        (rand() % 100) <= 78, 2,  -- 13% US
        (rand() % 100) <= 88, 3,  -- 10% Brazil
        (rand() % 7) + 4          -- 12% others
    )),
    
    -- Injected Profile Tier
    injected_profile_tier enum8(
        'Bronze' = 1, 'Silver' = 2, 'Gold' = 3, 'Platinum' = 4,
        'Diamond' = 5, 'Steel' = 6, 'Ruby' = 7, 'Emerald' = 8
    ) DEFAULT array_element([
        'Bronze', 'Silver', 'Gold', 'Platinum',
        'Diamond', 'Steel', 'Ruby', 'Emerald'
    ], multi_if(
        (rand() % 100) <= 25, 1,  -- 25% Bronze
        (rand() % 100) <= 45, 2,  -- 20% Silver
        (rand() % 100) <= 60, 3,  -- 15% Gold
        (rand() % 100) <= 72, 4,  -- 12% Platinum
        (rand() % 100) <= 82, 5,  -- 10% Diamond
        (rand() % 100) <= 90, 6,  -- 8% Steel
        (rand() % 100) <= 96, 7,  -- 6% Ruby
        8                         -- 4% Emerald
    )),
    
    -- Injected App Type
    injected_app_type enum8(
        'CASH' = 1, 'ANDROID' = 2, 'IOS' = 3, 'WEB' = 4, 'TABLET' = 5
    ) DEFAULT array_element([
        'CASH', 'ANDROID', 'IOS', 'WEB', 'TABLET'
    ], multi_if(
        (rand() % 100) <= 50, 1,  -- 50% CASH
        (rand() % 100) <= 72, 2,  -- 22% ANDROID
        (rand() % 100) <= 90, 3,  -- 18% IOS
        (rand() % 100) <= 96, 4,  -- 6% WEB
        5                         -- 4% TABLET
    )),
    
    -- Mobile Number
    mobile_number string DEFAULT concat('+91', to_string(rand() % 9000000000 + 1000000000)),
    
    -- User ID
    user_id_duplicate string DEFAULT to_string(rand() % 999999999 + 100000000),
    
    -- Injected Language Code
    injected_language_code enum8(
        'en_IN' = 1, 'hi_IN' = 2, 'en_US' = 3, 'pt_BR' = 4,
        'en_GB' = 5, 'fr_FR' = 6, 'de_DE' = 7, 'zh_CN' = 8
    ) DEFAULT array_element([
        'en_IN', 'hi_IN', 'en_US', 'pt_BR',
        'en_GB', 'fr_FR', 'de_DE', 'zh_CN'
    ], multi_if(
        (rand() % 100) <= 55, 1,  -- 55% English India
        (rand() % 100) <= 70, 2,  -- 15% Hindi India
        (rand() % 100) <= 80, 3,  -- 10% English US
        (rand() % 100) <= 87, 4,  -- 7% Portuguese Brazil
        (rand() % 4) + 5          -- 13% others
    )),
    
    -- ctr_received_timestamp
    ctr_received_timestamp string DEFAULT to_string(rand() % 86400000 + 1755586575732),
    
    -- dis_received_timestamp
    dis_received_timestamp string DEFAULT to_string(rand() % 86400000 + 1755586575733),
    
    -- dis_unique_uuid
    dis_unique_uuid string DEFAULT lower(hex(uuid()))
    
) SETTINGS eps = 30;


CREATE RANDOM STREAM game.user_account_balance_updated (
    -- Type
    type enum8('DEBIT' = 1, 'CREDIT' = 2) DEFAULT array_element(['DEBIT', 'CREDIT'], multi_if(
        (rand() % 100) <= 60, 1,  -- 60% DEBIT
        2                         -- 40% CREDIT
    )),
    
    -- User Id
    user_id string DEFAULT to_string(rand() % 999999 + 100),
    
    -- Description
    description string DEFAULT array_element([
        'Account Maintenance Fee ',
        'Game Entry Fee',
        'Tournament Prize',
        'Bonus Credit',
        'Withdrawal Fee',
        'Referral Bonus',
        'Cashback Credit',
        'Game Winnings',
        'Deposit Bonus',
        'Service Charge'
    ], (rand() % 10) + 1),
    
    -- Net Amount
    net_amount float32 DEFAULT multi_if(
        type = 'DEBIT', -round(rand_uniform(1.0, 500.0), 2),
        round(rand_uniform(1.0, 1000.0), 2)
    ),
    
    -- Reference Id
    reference_id string DEFAULT array_element([
        'Annual_maintaince_fee',
        'game_entry_12345',
        'tournament_win_67890',
        'bonus_credit_abc123',
        'withdrawal_fee_xyz789',
        'referral_bonus_ref456',
        'cashback_cb789',
        'game_win_gw123',
        'deposit_bonus_dep456',
        'service_charge_sc789'
    ], (rand() % 10) + 1),
    
    -- Amount
    amount float32 DEFAULT round(rand_uniform(1.0, 500.0), 2),
    
    -- Account Type
    account_type enum8(
        'Winnings Withdrawable' = 1, 
        'Bonus Cash' = 2, 
        'Deposit Balance' = 3,
        'Tournament Winnings' = 4,
        'Cashback Balance' = 5,
        'Promotional Balance' = 6
    ) DEFAULT array_element([
        'Winnings Withdrawable', 'Bonus Cash', 'Deposit Balance',
        'Tournament Winnings', 'Cashback Balance', 'Promotional Balance'
    ], multi_if(
        (rand() % 100) <= 45, 1,  -- 45% Winnings Withdrawable
        (rand() % 100) <= 65, 2,  -- 20% Bonus Cash
        (rand() % 100) <= 80, 3,  -- 15% Deposit Balance
        (rand() % 100) <= 90, 4,  -- 10% Tournament Winnings
        (rand() % 100) <= 96, 5,  -- 6% Cashback Balance
        6                         -- 4% Promotional Balance
    )),
    
    -- Transaction Id
    transaction_id string DEFAULT concat(
        random_printable_ascii(7),
        '_',
        random_printable_ascii(10)
    ),
    
    -- Is Success
    is_success bool DEFAULT rand_bernoulli(0.95),  -- 95% successful transactions
    
    -- Injected Country Code
    injected_country_code enum8(
        'IN' = 1, 'US' = 2, 'BR' = 3, 'GB' = 4, 'CA' = 5,
        'AU' = 6, 'DE' = 7, 'FR' = 8, 'SG' = 9, 'UAE' = 10
    ) DEFAULT array_element([
        'IN', 'US', 'BR', 'GB', 'CA', 'AU', 'DE', 'FR', 'SG', 'UAE'
    ], multi_if(
        (rand() % 100) <= 70, 1,  -- 70% India
        (rand() % 100) <= 82, 2,  -- 12% US
        (rand() % 100) <= 90, 3,  -- 8% Brazil
        (rand() % 7) + 4          -- 10% others
    )),
    
    -- Injected Profile Tier
    injected_profile_tier enum8(
        'Bronze' = 1, 'Silver' = 2, 'Gold' = 3, 'Platinum' = 4,
        'Diamond' = 5, 'Steel' = 6, 'Ruby' = 7, 'Emerald' = 8
    ) DEFAULT array_element([
        'Bronze', 'Silver', 'Gold', 'Platinum',
        'Diamond', 'Steel', 'Ruby', 'Emerald'
    ], multi_if(
        (rand() % 100) <= 30, 1,  -- 30% Bronze
        (rand() % 100) <= 50, 2,  -- 20% Silver
        (rand() % 100) <= 65, 3,  -- 15% Gold
        (rand() % 100) <= 77, 4,  -- 12% Platinum
        (rand() % 100) <= 86, 5,  -- 9% Diamond
        (rand() % 100) <= 92, 6,  -- 6% Steel
        (rand() % 100) <= 97, 7,  -- 5% Ruby
        8                         -- 3% Emerald
    )),
    
    -- Injected App Type
    injected_app_type enum8(
        'CASH' = 1, 'ANDROID' = 2, 'IOS' = 3, 'WEB' = 4, 'TABLET' = 5
    ) DEFAULT array_element([
        'CASH', 'ANDROID', 'IOS', 'WEB', 'TABLET'
    ], multi_if(
        (rand() % 100) <= 55, 1,  -- 55% CASH
        (rand() % 100) <= 75, 2,  -- 20% ANDROID
        (rand() % 100) <= 90, 3,  -- 15% IOS
        (rand() % 100) <= 97, 4,  -- 7% WEB
        5                         -- 3% TABLET
    )),
    
    -- Mobile Number
    mobile_number string DEFAULT concat('+91', to_string(rand() % 900000000 + 100000000)),
    
    -- User ID (duplicate field)
    user_id_duplicate string DEFAULT to_string(rand() % 999999 + 100),
    
    -- Injected Language Code
    injected_language_code enum8(
        'en_IN' = 1, 'hi_IN' = 2, 'en_US' = 3, 'pt_BR' = 4,
        'en_GB' = 5, 'fr_FR' = 6, 'de_DE' = 7, 'zh_CN' = 8,
        'ta_IN' = 9, 'te_IN' = 10
    ) DEFAULT array_element([
        'en_IN', 'hi_IN', 'en_US', 'pt_BR', 'en_GB',
        'fr_FR', 'de_DE', 'zh_CN', 'ta_IN', 'te_IN'
    ], multi_if(
        (rand() % 100) <= 45, 1,  -- 45% English India
        (rand() % 100) <= 65, 2,  -- 20% Hindi India
        (rand() % 100) <= 73, 3,  -- 8% English US
        (rand() % 100) <= 80, 4,  -- 7% Portuguese Brazil
        (rand() % 100) <= 85, 9,  -- 5% Tamil India
        (rand() % 100) <= 90, 10, -- 5% Telugu India
        (rand() % 4) + 5          -- 10% others
    ))
    
) SETTINGS eps = 200;


-- Stream

CREATE STREAM game.user_won_stream
(
  `eventName` string,
  `partition` string,
  `offset` string,
  `kafka_time` datetime64(3, 'UTC'),
  `event_time` datetime64(3,'UTC'),
  `user_id` string,
  `game_id` bigint,
  `cash_entry_fee` float64
) SETTINGS
event_time_column = 'event_time';


-- Materialized View
CREATE MATERIALIZED VIEW game.user_won_mv TO game.user_won_stream AS
SELECT
    'user_won' AS eventName,
    toString(rand() % 10) AS partition,
    toString(rand() % 1000000) AS offset,
    _tp_time AS kafka_time,
    _tp_time AS event_time,
    user_id,
    game_id,
    cash_entry_fee
FROM
    game.user_won