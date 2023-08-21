SELECT count(distinct v:actor.login) FROM github_events WHERE v:type = 'WatchEvent'
