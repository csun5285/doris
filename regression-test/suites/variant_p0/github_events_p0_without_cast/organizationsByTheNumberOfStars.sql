SELECT
    lower(split_part(v:repo.name, '/', 1)) AS org,
    count() AS stars
FROM github_events
WHERE v:type = 'WatchEvent'
GROUP BY org
ORDER BY stars DESC, 1
LIMIT 50
