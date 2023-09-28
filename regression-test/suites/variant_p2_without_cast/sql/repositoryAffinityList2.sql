SELECT
  repo_name,
  total_stars,
  round(spark_stars / total_stars, 2) AS ratio
FROM
(
    SELECT
        repo:name as repo_name,
        count(distinct actor:login ) AS total_stars
    FROM github_events
    WHERE (type = 'WatchEvent') AND (repo:name NOT IN ('apache/spark'))
    GROUP BY repo_name
    HAVING total_stars >= 10
) t1
JOIN
(
    SELECT
        count(distinct actor:login ) AS spark_stars
    FROM github_events
    WHERE (type = 'WatchEvent') AND (repo:name IN ('apache/spark'))
) t2
ORDER BY ratio DESC, repo_name
LIMIT 50
