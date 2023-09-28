SELECT actor:login , count() AS stars FROM github_events WHERE type = 'WatchEvent' AND actor:login  = 'cliffordfajardo' GROUP BY actor:login  ORDER BY stars DESC LIMIT 50
