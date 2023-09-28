SELECT actor:login , count() AS stars FROM github_events WHERE type = 'WatchEvent' GROUP BY actor:login  ORDER BY stars DESC, 1 LIMIT 50
