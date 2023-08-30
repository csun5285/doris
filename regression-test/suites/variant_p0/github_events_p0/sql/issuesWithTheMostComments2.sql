SELECT v:repo.name, count() FROM github_events WHERE v:type = 'IssueCommentEvent' GROUP BY v:repo.name ORDER BY count() DESC, 1 LIMIT 50
