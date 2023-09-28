SELECT
    repo:name,
    cast(payload:issue.number as int)  as number,
    count() AS comments
FROM github_events
WHERE type = 'IssueCommentEvent' AND (payload:action = 'created') AND (cast(payload:issue.number as int)  > 10)
GROUP BY repo:name, number
ORDER BY comments DESC, repo:name, number
LIMIT 50
