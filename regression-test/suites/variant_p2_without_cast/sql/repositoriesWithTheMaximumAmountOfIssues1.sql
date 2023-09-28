SELECT repo:name, count() AS c, count(distinct actor:login ) AS u FROM github_events WHERE type = 'IssuesEvent' AND payload:action = 'opened' GROUP BY repo:name ORDER BY c DESC, repo:name LIMIT 50
