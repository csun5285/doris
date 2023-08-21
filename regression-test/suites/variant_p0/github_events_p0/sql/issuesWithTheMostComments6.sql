SELECT
    v:repo.name,
    v:payload.issue.`number`  as number,
    count() AS comments,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE v:type = 'IssueCommentEvent' AND (v:payload.action = 'created') AND (v:payload.issue.`number` > 10)
GROUP BY v:repo.name, number
HAVING authors >= 4
ORDER BY comments DESC, v:repo.name
LIMIT 50
