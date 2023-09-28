SELECT
    repo_name,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        v:repo.name as repo_name,
        CASE WHEN v:type = 'PushEvent' AND (v:payload.ref LIKE '%/master' OR v:payload.ref LIKE '%/main') THEN v:actor.login ELSE NULL END AS actor_login,
        CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events WHERE v:type IN ('PushEvent', 'WatchEvent') AND v:repo.name != '/'
) t
GROUP BY repo_name ORDER BY u DESC, repo_name LIMIT 5
