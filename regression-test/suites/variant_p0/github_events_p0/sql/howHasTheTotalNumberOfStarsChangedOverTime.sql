SELECT year(v:created_at) AS year, count() AS stars FROM github_events WHERE v:type = 'WatchEvent' GROUP BY year ORDER BY year

