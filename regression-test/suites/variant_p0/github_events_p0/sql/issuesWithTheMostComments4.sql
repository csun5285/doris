SELECT
    v:repo.name,
    v:payload.issue.`number`  as number,
    count() AS comments
FROM github_events
WHERE v:type = 'IssueCommentEvent' AND (v:payload.action = 'created')
GROUP BY v:repo.name, number 
ORDER BY comments DESC, number ASC, 1
LIMIT 50
