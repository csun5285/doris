SELECT
    concat('https://github.com/', repo:name, '/pull/') AS URL,
    count(distinct actor:login ) AS authors
FROM github_events
WHERE (type = 'PullRequestReviewCommentEvent') AND (payload:action = 'created')
GROUP BY
    repo:name,
    cast(payload:issue.`number` as string) 
ORDER BY authors DESC, URL ASC
LIMIT 50