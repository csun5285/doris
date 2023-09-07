-- ERROR: unmatched column
SELECT
  cast(repo:name as string) as repo_name,
  count() AS stars
FROM github_events
WHERE (type = 'WatchEvent') AND (cast(actor:login as string) IN
(
    SELECT cast(actor:login as string)
    FROM github_events
    WHERE (type = 'WatchEvent') AND (cast(repo:name as string) IN ('apache/spark', 'prakhar1989/awesome-courses'))
)) AND (cast(repo:name as string) NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
GROUP BY repo_name
ORDER BY stars DESC, repo_name 
LIMIT 50
