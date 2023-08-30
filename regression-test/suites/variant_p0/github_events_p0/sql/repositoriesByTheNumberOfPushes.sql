SELECT
    v:repo.name,
    count() AS pushes,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'PushEvent') AND (v:repo.name IN
(
    SELECT v:repo.name
    FROM github_events
    WHERE v:type = 'WatchEvent'
    GROUP BY v:repo.name
    ORDER BY count() DESC
    LIMIT 10000
))
GROUP BY v:repo.name
ORDER BY count() DESC
LIMIT 50
