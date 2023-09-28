SELECT
    cast(pow(10, floor(log10(c))) as int) AS stars,
    count(distinct k)
FROM
(
    SELECT
        repo:name as k,
        count() AS c
    FROM github_events
    WHERE type = 'WatchEvent'
    GROUP BY repo:name
) t
GROUP BY stars
ORDER BY stars ASC
