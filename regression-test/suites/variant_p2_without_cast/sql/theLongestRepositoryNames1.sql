SELECT count(), repo:name FROM github_events WHERE type = 'WatchEvent' GROUP BY repo:name ORDER BY length(repo:name) DESC, repo:name LIMIT 50
