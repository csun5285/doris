SELECT
    repo:name,
    payload:issue.`number`  as number,
    count() AS comments
FROM github_events
WHERE type = 'IssueCommentEvent' AND (payload:action = 'created')
GROUP BY repo:name, number 
ORDER BY comments DESC, number ASC, 1
LIMIT 50

