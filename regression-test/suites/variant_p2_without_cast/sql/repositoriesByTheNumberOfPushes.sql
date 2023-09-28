SELECT
    repo:name,
    count() AS pushes,
    count(distinct actor:login ) AS authors
FROM github_events
WHERE (type = 'PushEvent') AND (repo:name IN
(
    SELECT repo:name
    FROM github_events
    WHERE type = 'WatchEvent'
    GROUP BY repo:name
    ORDER BY count() DESC
    LIMIT 10000
))
GROUP BY repo:name
ORDER BY count() DESC
LIMIT 50
