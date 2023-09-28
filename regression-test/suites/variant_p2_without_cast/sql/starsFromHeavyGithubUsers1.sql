SELECT
    repo:name,
    count()
FROM github_events
WHERE (type = 'WatchEvent') AND (actor:login  IN
(
    SELECT actor:login 
    FROM github_events
    WHERE (type = 'PullRequestEvent') AND (payload:action = 'opened')
))
GROUP BY repo:name
ORDER BY count() DESC, repo:name
LIMIT 50
