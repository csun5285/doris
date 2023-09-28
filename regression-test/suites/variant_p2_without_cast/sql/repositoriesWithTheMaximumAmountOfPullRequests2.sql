SELECT repo:name, count(), count(distinct actor:login ) AS u FROM github_events WHERE type = 'PullRequestEvent' AND payload:action = 'opened' GROUP BY repo:name ORDER BY u DESC, 2 DESC, 1 LIMIT 50
