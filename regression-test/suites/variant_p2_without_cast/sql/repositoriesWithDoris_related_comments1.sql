SELECT repo:name, count() FROM github_events WHERE lower(payload:comment.body) LIKE '%spark%' GROUP BY repo:name ORDER BY count() DESC, repo:name ASC LIMIT 50