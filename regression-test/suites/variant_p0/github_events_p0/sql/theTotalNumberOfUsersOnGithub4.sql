SELECT count(distinct v:actor.login) FROM github_events WHERE v:type = 'PullRequestEvent' AND v:payload.action = 'opened'
