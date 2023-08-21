SELECT
    concat('https://github.com/', v:repo.name, '/pull/') AS URL,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'PullRequestReviewCommentEvent') AND (v:payload.action = 'created')
GROUP BY
    v:repo.name,
    cast(v:payload.issue.`number` as string) 
ORDER BY authors DESC, URL ASC
LIMIT 50