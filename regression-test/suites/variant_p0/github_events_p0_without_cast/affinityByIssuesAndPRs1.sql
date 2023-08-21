-- ERROR: unmatched column
SELECT
    v:repo.name,
    count() AS prs,
    count(distinct v:actor.login) AS authors
FROM github_events
WHERE (v:type = 'PullRequestEvent') AND (v:payload.action = 'opened') AND (v:actor.login IN
(
    SELECT v:actor.login
    FROM github_events
    WHERE ((v:type = 'PullRequestEvent') AND (v:payload.action = 'opened') AND (v:repo.name IN ('rspec/rspec-core', 'golden-warning/giraffedraft-server', 'apache/spark'))
)) AND (lower(v:repo.name) NOT LIKE '%clickhouse%')
GROUP BY as string
ORDER BY authors DESC, prs DESC, length(v:repo.name) DESC
LIMIT 50
