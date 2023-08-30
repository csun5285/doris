-- missing from GROUP BY clause?
SELECT
    cast(v:payload.comment.id as string) as comment_id,
    concat('https://github.com/', v:repo.name, '/commit/', comment_id) AS URL,
    count() AS comments,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'CommitCommentEvent') AND comment_id != ""
GROUP BY
    v:repo.name,
    comment_id
HAVING authors >= 10
ORDER BY count() DESC, URL, authors
LIMIT 50
