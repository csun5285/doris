SELECT
    repo_name,
    sum(created_at_2022) AS stars2022,
    sum(created_at_2015) AS stars2015,
    round(sum(created_at_2022) / sum(created_at_2015), 3) AS yoy,
    min(created_at) AS first_seen
FROM
(
    SELECT
        v:repo.name as repo_name,
        CASE year(v:created_at) WHEN 2022 THEN 1 ELSE 0 END AS created_at_2022,
        CASE year(v:created_at) WHEN 2015 THEN 1 ELSE 0 END AS created_at_2015,
        v:created_at as created_at
    FROM github_events
    WHERE v:type = 'WatchEvent'
) t
GROUP BY  repo_name 
HAVING (min(created_at) <= '2023-01-01 00:00:00') AND (stars2022 >= 1) and (stars2015 >= 1)
ORDER BY yoy DESC, repo_name 
LIMIT 50
