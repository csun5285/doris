-- ERROR: unmatched column
SELECT
    cast(repo:name as string),
    count() AS prs,
    count(distinct cast(actor:login as string)) AS authors
FROM github_events
WHERE (type = 'IssuesEvent') AND (cast(payload:action as string) = 'opened') AND (cast(actor:login as string) IN
(
    SELECT cast(actor:login as string)
    FROM github_events
    WHERE (type = 'IssuesEvent') AND (cast(payload:action as string) = 'opened') AND (cast(repo:name as string) IN ('No-CQRT/GooGuns', 'ivolunteerph/ivolunteerph', 'Tribler/tribler'))
)) AND (lower(cast(repo:name as string)) NOT LIKE '%clickhouse%')
GROUP BY cast(repo:name as string)
ORDER BY authors DESC, prs DESC, cast(repo:name as string) ASC
LIMIT 50
