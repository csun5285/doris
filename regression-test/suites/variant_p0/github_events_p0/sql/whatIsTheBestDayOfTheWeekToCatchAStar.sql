SELECT dayofweek(v:created_at) AS day, count() AS stars FROM github_events WHERE v:type = 'WatchEvent' GROUP BY day ORDER BY day
