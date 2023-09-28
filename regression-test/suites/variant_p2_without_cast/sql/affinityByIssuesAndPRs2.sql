SELECT
    repo:name,
    count() AS prs,
    count(distinct actor:login ) AS authors
FROM github_events
WHERE (type = 'IssuesEvent') AND (payload:action = 'opened') AND (actor:login  IN
(
    SELECT actor:login 
    FROM github_events
    WHERE (type = 'IssuesEvent') AND (payload:action = 'opened') AND (repo:name IN ('No-CQRT/GooGuns', 'ivolunteerph/ivolunteerph', 'Tribler/tribler'))
)) AND (lower(repo:name) NOT LIKE '%clickhouse%')
GROUP BY repo:name
ORDER BY authors DESC, prs DESC, repo:name ASC
LIMIT 50
