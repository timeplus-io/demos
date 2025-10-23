-- Alert UDF Definition
CREATE OR REPLACE FUNCTION alert_to_slack(value string) RETURNS string LANGUAGE PYTHON AS $$
import json
import requests
def alert_to_slack(value):
    result = ""
    for val in value:
        result += f"{val}\n"
    requests.post("https://hooks.slack.com/services/***", data=json.dumps({"text": f"{result}"}))
    return value
$$

-- Alert if a player has lost 5 games in a row and spent more than $100 in last 10 transactions

CREATE ALERT spending_alert
BATCH 10 EVENTS WITH TIMEOUT 5s
LIMIT 1 ALERTS PER 15s
CALL alert_to_slack
AS 
SELECT concat(user_id, ' spend ', to_string(total_spend), ' in last 10 games') as value 
FROM game.total_spend_last_10_transaction
WHERE total_spend > 750


-- 
DROP ALERT spending_alert