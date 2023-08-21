SELECT
    v:repo.name,
    count() AS comments,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE v:type = 'CommitCommentEvent'
GROUP BY v:repo.name
ORDER BY count() DESC, 1, 3
LIMIT 50
