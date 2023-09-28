SELECT repo:name, count() FROM github_events WHERE type = 'IssueCommentEvent' GROUP BY repo:name ORDER BY count() DESC, 1 LIMIT 50
