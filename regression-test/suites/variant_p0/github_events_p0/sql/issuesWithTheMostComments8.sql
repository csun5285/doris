SELECT
    cast(v:payload.comment.id as string) as comment_id,
    concat('https://github.com/', v:repo.name, '/commit/', cast(v:payload.comment.id as string)) AS URL,
    count() AS comments,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'CommitCommentEvent') AND cast(v:payload.comment.id as string) != ""
GROUP BY
    v:repo.name,
    cast(v:payload.comment.id as string)
HAVING authors >= 0
ORDER BY comments DESC, URL, authors, comment_id
LIMIT 5
