SELECT
    repo:name,
    count()
FROM github_events
WHERE (type = 'WatchEvent') AND (actor:login  IN
(
    SELECT actor:login 
    FROM github_events
    WHERE (type = 'PullRequestEvent') AND (payload:action = 'opened')
    GROUP BY actor:login 
    HAVING count() >= 2
))
GROUP BY repo:name
ORDER BY 1, count() DESC, 1
LIMIT 50
