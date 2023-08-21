SELECT
    v:repo.name,
    count()
FROM github_events
WHERE (v:type = 'WatchEvent') AND (v:actor.login IN
(
    SELECT v:actor.login
    FROM github_events
    WHERE (v:type = 'PullRequestEvent') AND (v:payload.action = 'opened')
))
GROUP BY v:repo.name
ORDER BY count() DESC, v:repo.name
LIMIT 50
