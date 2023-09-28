SELECT repo:name as repo_name, count() AS stars FROM github_events WHERE type = 'WatchEvent' GROUP BY repo:name ORDER BY stars DESC, repo_name LIMIT 50
