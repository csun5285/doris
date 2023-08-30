SELECT
    repo_name,
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 3) AS ratio
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
ORDER BY forks DESC, 1, 3, 4
LIMIT 50
