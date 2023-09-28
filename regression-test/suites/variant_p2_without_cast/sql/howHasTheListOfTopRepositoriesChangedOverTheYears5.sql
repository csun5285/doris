SELECT repo:name, count() AS stars FROM github_events WHERE type = 'WatchEvent' AND year(created_at) = '2015' GROUP BY repo:name ORDER BY stars DESC, 1 LIMIT 50
