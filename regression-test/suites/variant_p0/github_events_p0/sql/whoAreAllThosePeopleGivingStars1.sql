SELECT v:actor.login, count() AS stars FROM github_events WHERE v:type = 'WatchEvent' GROUP BY v:actor.login ORDER BY stars DESC, 1 LIMIT 50
