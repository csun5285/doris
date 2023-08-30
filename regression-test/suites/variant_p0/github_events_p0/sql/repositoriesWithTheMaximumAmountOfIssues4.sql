SELECT
    repo_name,
    sum(issue_created) AS c,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        v:repo.name as repo_name,
        CASE WHEN (v:type = 'IssuesEvent') AND (v:payload.action = 'opened') THEN 1 ELSE 0 END AS issue_created,
        CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star,
        CASE WHEN (v:type = 'IssuesEvent') AND (v:payload.action = 'opened') THEN v:actor.login ELSE NULL END AS actor_login 
    FROM github_events
    WHERE v:type IN ('IssuesEvent', 'WatchEvent')
) t
GROUP BY repo_name
ORDER BY u, c, stars DESC, 1
LIMIT 50
