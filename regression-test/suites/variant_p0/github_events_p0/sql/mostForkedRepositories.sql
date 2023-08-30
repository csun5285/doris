SELECT v:repo.name, count() AS forks FROM github_events WHERE v:type = 'ForkEvent' GROUP BY v:repo.name ORDER BY forks DESC, 1 LIMIT 50
