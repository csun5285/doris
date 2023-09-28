SELECT
    repo_name,
    stars
FROM
(
    SELECT
        row_number() OVER (PARTITION BY repo_name ORDER BY stars DESC) AS rank,
        repo_name,
        stars
    FROM
    (
        SELECT
            v:repo.name as repo_name,
            count() AS stars
        FROM github_events
        WHERE v:type = 'WatchEvent'
        GROUP BY repo_name
    ) t1
) t2
WHERE rank = 1
ORDER BY stars DESC, repo_name
LIMIT 50
