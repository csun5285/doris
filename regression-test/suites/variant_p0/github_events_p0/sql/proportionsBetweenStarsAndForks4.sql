SELECT
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 2) AS ratio
FROM
(
    SELECT
        v:repo.name,
        CASE WHEN v:type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE v:type IN ('ForkEvent', 'WatchEvent')
) t
