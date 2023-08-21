SELECT v:repo.name, count() FROM github_events WHERE lower(v:payload.comment.body) LIKE '%spark%' GROUP BY v:repo.name ORDER BY count() DESC, v:repo.name ASC LIMIT 50