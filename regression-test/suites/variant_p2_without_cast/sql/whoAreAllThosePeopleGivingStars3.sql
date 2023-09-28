SELECT
    repo:name,
    count() AS stars
FROM github_events
WHERE (type = 'WatchEvent') AND (repo:name IN
(
    SELECT repo:name
    FROM github_events
    WHERE (type = 'WatchEvent') AND (actor:login  = 'cliffordfajardo')
))
GROUP BY repo:name
ORDER BY stars DESC, repo:name
LIMIT 50



