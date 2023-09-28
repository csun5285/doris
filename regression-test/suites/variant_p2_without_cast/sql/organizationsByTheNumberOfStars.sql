SELECT
    lower(split_part(repo_name, '/', 1)) AS org,
    count() AS stars
FROM (
     SELECT repo:name as repo_name
    FROM github_events
    WHERE type = 'WatchEvent'
) t
GROUP BY org
ORDER BY stars DESC, 1
LIMIT 50

