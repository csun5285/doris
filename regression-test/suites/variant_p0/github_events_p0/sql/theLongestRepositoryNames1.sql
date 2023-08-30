SELECT count(), v:repo.name FROM github_events WHERE v:type = 'WatchEvent' GROUP BY v:repo.name ORDER BY length(v:repo.name) DESC, v:repo.name LIMIT 50
