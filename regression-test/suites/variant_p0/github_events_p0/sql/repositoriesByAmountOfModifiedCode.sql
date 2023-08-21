SELECT
    v:repo.name as repo_name,
    count() AS prs,
    count(distinct v:actor.login) AS authors,
    sum(v:payload.pull_request.additions) AS adds,
    sum(v:payload.pull_request.deletions) AS dels
FROM github_events
WHERE (v:type = 'PullRequestEvent') AND (v:payload.action = 'opened') AND (v:payload.pull_request.additions < 10000) AND (v:payload.pull_request.deletions < 10000)
GROUP BY repo_name
HAVING (adds / dels) < 10
ORDER BY adds + dels DESC, 1
LIMIT 50
