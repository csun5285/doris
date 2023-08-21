SELECT v:repo.name, count() AS stars FROM github_events WHERE v:type = 'WatchEvent' AND year(v:created_at) = '2015' GROUP BY v:repo.name ORDER BY stars DESC, 1 LIMIT 50
