SELECT repo:name, count() AS stars FROM github_events WHERE type = 'WatchEvent' GROUP BY repo:name ORDER BY stars DESC, 1 LIMIT 50
