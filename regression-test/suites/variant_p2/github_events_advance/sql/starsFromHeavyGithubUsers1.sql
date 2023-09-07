-- ERROR:  unmatched column
SELECT
    cast(repo:name as string),
    count()
FROM github_events
WHERE (type = 'WatchEvent') AND (cast(actor:login as string) IN
(
    SELECT cast(actor:login as string)
    FROM github_events
    WHERE (type = 'PullRequestEvent') AND (cast(payload:action as string) = 'opened')
))
GROUP BY cast(repo:name as string)
ORDER BY count() DESC
LIMIT 50
