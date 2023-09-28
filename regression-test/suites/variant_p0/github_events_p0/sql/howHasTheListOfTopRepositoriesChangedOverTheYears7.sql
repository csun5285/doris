SELECT
    repo,
    year,
    cnt
FROM
(
    SELECT
        row_number() OVER (PARTITION BY year ORDER BY cnt DESC) AS r,
        repo,
        year,
        cnt
    FROM
    (
        SELECT
        lower(v:repo.name) AS repo,
        year(v:created_at) AS year,
        count() AS cnt
        FROM github_events
        WHERE (v:type = 'WatchEvent') AND (year(v:created_at) >= 2015)
        GROUP BY
            repo,
            year
    ) t
) t2
WHERE r <= 10
ORDER BY
    year ASC,
    cnt DESC,
    repo
limit 5
