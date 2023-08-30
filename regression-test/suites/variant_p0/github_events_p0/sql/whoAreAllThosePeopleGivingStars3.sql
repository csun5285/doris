SELECT
    v:repo.name,
    count() AS stars
FROM github_events
WHERE (v:type = 'WatchEvent') AND (v:repo.name IN
(
    SELECT v:repo.name
    FROM github_events
    WHERE (v:type = 'WatchEvent') AND (v:actor.login = 'cliffordfajardo')
))
GROUP BY v:repo.name
ORDER BY stars DESC
LIMIT 50
