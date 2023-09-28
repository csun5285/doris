SELECT
    repo_name,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        repo:name as repo_name,
        CASE WHEN type = 'PushEvent' AND (payload:ref  LIKE '%/master' OR payload:ref  LIKE '%/main') THEN actor:login  ELSE NULL END AS actor_login,
        CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events WHERE type IN ('PushEvent', 'WatchEvent') AND repo:name != '/'
) t
GROUP BY repo_name
HAVING stars >= 100
ORDER BY u DESC, repo_name
LIMIT 50;
