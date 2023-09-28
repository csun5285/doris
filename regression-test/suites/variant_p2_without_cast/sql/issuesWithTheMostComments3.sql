SELECT
    repo_name,
    comments,
    issues,
    cast(round(comments / issues, 0) as int) AS ratio
FROM
(
    SELECT
        repo:name as repo_name,
        count() AS comments,
        count(distinct payload:issue.`number`) AS issues
    FROM github_events
    WHERE type = 'IssueCommentEvent'
    GROUP BY repo:name
) t
ORDER BY comments DESC, 1, 3, 4
LIMIT 50
