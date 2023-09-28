SELECT repo:name, count() AS forks FROM github_events WHERE type = 'ForkEvent' GROUP BY repo:name ORDER BY forks DESC, 1 LIMIT 50
