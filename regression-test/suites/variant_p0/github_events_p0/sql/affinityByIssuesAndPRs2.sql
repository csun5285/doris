SELECT
    v:repo.name,
    count() AS prs,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'IssuesEvent') AND (v:payload.action = 'opened') AND (v:actor.login IN
(
    SELECT v:actor.login
    FROM github_events
    WHERE (v:type = 'IssuesEvent') AND (v:payload.action = 'opened') AND (v:repo.name IN ('No-CQRT/GooGuns', 'ivolunteerph/ivolunteerph', 'Tribler/tribler'))
)) AND (lower(v:repo.name) NOT LIKE '%clickhouse%')
GROUP BY v:repo.name
ORDER BY authors DESC, prs DESC, v:repo.name ASC
LIMIT 50
