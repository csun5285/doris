SELECT
    repo_name,
    max(stars) AS daily_stars,
    sum(stars) AS total_stars,
    sum(stars) / max(stars) AS rate
FROM
(
    SELECT
        v:repo.name as repo_name,
        to_date(v:created_at) AS day,
        count() AS stars
    FROM github_events
    WHERE v:type = 'WatchEvent'
    GROUP BY
        repo_name,
        day
) t
GROUP BY repo_name
ORDER BY rate DESC, 1
LIMIT 50
