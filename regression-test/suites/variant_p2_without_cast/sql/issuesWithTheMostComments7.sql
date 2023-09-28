SELECT
    repo:name,
    count() AS comments,
    count(distinct actor:login ) AS authors
FROM github_events
WHERE type = 'CommitCommentEvent'
GROUP BY repo:name
ORDER BY count() DESC, 1, 3
LIMIT 50
