SELECT
    repo_name,
    day,
    stars
FROM
(
    SELECT
        row_number() OVER (PARTITION BY repo_name  ORDER BY stars DESC) AS rank,
        repo_name,
        day,
        stars
    FROM
    (
        SELECT
            v:repo.name as repo_name,
            to_date(v:created_at) AS day,
            count() AS stars
        FROM github_events
        WHERE v:type = 'WatchEvent'
        GROUP BY v:repo.name, day
    ) t1
) t2
WHERE rank = 1
ORDER BY stars DESC, 1
LIMIT 50
