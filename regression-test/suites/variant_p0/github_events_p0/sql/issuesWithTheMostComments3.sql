SELECT
    repo_name,
    comments,
    issues,
    round(comments / issues, 2) AS ratio
FROM
(
    SELECT
        v:repo.name as repo_name,
        count() AS comments,
        count(distinct v:payload.issue.`number`) AS issues
    FROM github_events
    WHERE v:type = 'IssueCommentEvent'
    GROUP BY v:repo.name
) t
ORDER BY comments DESC, 1, 3, 4
LIMIT 50
