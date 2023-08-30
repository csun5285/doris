SELECT
    repo_name,
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(fork) / sum(star), 2) AS ratio
FROM
(
    SELECT
        v:repo.name as repo_name,
        CASE WHEN v:type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE v:type IN ('ForkEvent', 'WatchEvent')
) t
GROUP BY repo_name
HAVING (stars > 4) AND (forks > 4)
ORDER BY ratio DESC
LIMIT 50
