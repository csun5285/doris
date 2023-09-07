-- ERROR missmatched result
SELECT
    cast(repo:name as string) as repo_name,
    count() AS prs,
    count(distinct cast(actor:login as string)) AS authors,
    sum(cast(payload:pull_request.additions as int)) AS adds,
    sum(cast(v:payload.pull_request.deletions as int)) AS dels
FROM github_events
WHERE (type = 'PullRequestEvent') AND (cast(payload:action as string) = 'opened') AND (cast(payload:pull_request.additions as int) < 10000) AND (cast(v:payload.pull_request.deletions as int) < 10000)
GROUP BY repo_name
HAVING (adds / dels) < 10
ORDER BY adds + dels DESC, 1
LIMIT 50
