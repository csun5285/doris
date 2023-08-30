SELECT v:repo.name, count() FROM github_events WHERE v:type = 'WatchEvent' AND v:repo.name LIKE '%_/_%' GROUP BY v:repo.name ORDER BY length(v:repo.name) ASC, v:repo.name LIMIT 50
