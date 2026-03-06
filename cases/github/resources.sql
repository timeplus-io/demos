
CREATE DATABASE IF NOT EXISTS github;

SYSTEM INSTALL PYTHON PACKAGE `PyGithub`;

CREATE EXTERNAL STREAM github.github_events_stream(
    id string,
    created_at string,
    actor string,
    type string,
    repo string,
    payload string
)
AS $$
import os
import time
from github import Github, GithubException

# Initialize outside the function to maintain state across calls if needed
token = os.environ.get("GITHUB_TOKEN")
g = Github(token, per_page=100) if token else None
known_ids = set()

def read_github():
    global g, known_ids
    if g is None:
        return

    while True:
        try:
            events = g.get_events()
            for e in events:
                if e.id not in known_ids:
                    known_ids.add(e.id)
                    # Yield a list where each element corresponds to a column
                    yield (
                        str(e.id),
                        e.created_at.isoformat(),
                        str(e.actor.login),
                        str(e.type),
                        str(e.repo.name),
                        str(e.payload)
                    )
            
            # Maintenance: Clear cache every 5000 IDs to manage memory
            if len(known_ids) > 5000:
                known_ids.clear()
                
            time.sleep(2)
            
        except GithubException:
            time.sleep(600) # API rate limit or error backoff
        except Exception:
            time.sleep(10)
$$
SETTINGS 
    type = 'python',
    read_function_name = 'read_github';


CREATE MATERIALIZED VIEW github.mv_github_events
AS
SELECT
  id, to_time(created_at) AS created_at, actor, type, repo, payload
FROM
  github.github_events_stream
STORAGE_SETTINGS index_granularity = 8192
TTL to_datetime(_tp_time) + 7d