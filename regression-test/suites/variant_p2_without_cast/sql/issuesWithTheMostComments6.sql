SELECT
    repo:name,
    payload:issue.`number`  as number,
    count() AS comments,
    count(distinct actor:login ) AS authors
FROM github_events
WHERE type = 'IssueCommentEvent' AND (payload:action = 'created') AND (payload:issue.`number` > 10)
GROUP BY repo:name, number
HAVING authors >= 4
ORDER BY comments DESC, number
LIMIT 50
