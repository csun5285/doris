SELECT repo:name, count() FROM github_events WHERE type = 'WatchEvent' AND repo:name LIKE '%_/_%' GROUP BY repo:name ORDER BY length(repo:name) ASC, repo:name LIMIT 50
