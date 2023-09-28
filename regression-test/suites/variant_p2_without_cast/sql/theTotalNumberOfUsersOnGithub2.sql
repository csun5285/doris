SELECT count(distinct actor:login ) FROM github_events WHERE type = 'WatchEvent'
