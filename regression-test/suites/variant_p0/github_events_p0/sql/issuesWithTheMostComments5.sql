SELECT
    v:repo.name,
    v:payload.issue.`number`  as number,
    count() AS comments
FROM github_events
WHERE v:type = 'IssueCommentEvent' AND (v:payload.action = 'created') AND (v:payload.issue.`number`  > 10)
GROUP BY v:repo.name, number
ORDER BY comments DESC, v:repo.name, number
LIMIT 50
