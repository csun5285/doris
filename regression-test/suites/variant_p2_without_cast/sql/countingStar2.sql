SELECT payload:action, count() FROM github_events WHERE type = 'WatchEvent' GROUP BY payload:action 