SELECT v:repo.name, count(), count(distinct v:actor.login) AS u FROM github_events WHERE v:type = 'PullRequestEvent' AND v:payload.action = 'opened' GROUP BY v:repo.name ORDER BY u DESC, 2 DESC, 1 LIMIT 50
