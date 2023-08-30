SELECT v:actor.login, count() AS stars FROM github_events WHERE v:type = 'WatchEvent' AND v:actor.login = 'cliffordfajardo' GROUP BY v:actor.login ORDER BY stars DESC LIMIT 50
