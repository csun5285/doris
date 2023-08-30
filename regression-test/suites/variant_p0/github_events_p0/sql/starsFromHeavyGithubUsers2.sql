SELECT
    v:repo.name,
    count()
FROM github_events
WHERE (v:type = 'WatchEvent') AND (v:actor.login IN
(
    SELECT v:actor.login
    FROM github_events
    WHERE (v:type = 'PullRequestEvent') AND (v:payload.action = 'opened')
    GROUP BY v:actor.login
    HAVING count() >= 5
))
GROUP BY v:repo.name
ORDER BY 1, count() DESC, 1
LIMIT 50
