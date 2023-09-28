SELECT
    repo:name as repo_name,
    count() AS prs,
    count(distinct actor:login ) AS authors,
    sum(payload:pull_request.additions) AS adds,
    sum(payload:pull_request.deletions) AS dels
FROM github_events
WHERE (type = 'PullRequestEvent') AND (payload:action = 'opened') AND (payload:pull_request.additions < 10000) AND (payload:pull_request.deletions < 10000)
GROUP BY repo_name
HAVING (adds / dels) < 10
ORDER BY adds + dels DESC, 1
LIMIT 50
