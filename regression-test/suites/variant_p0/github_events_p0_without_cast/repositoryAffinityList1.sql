-- ERROR: unmatched column
SELECT
  v:repo.name as repo_name,
  count() AS stars
FROM github_events
WHERE (v:type = 'WatchEvent') AND (v:actor.login IN
(
    SELECT v:actor.login
    FROM github_events
    WHERE (v:type = 'WatchEvent') AND (v:repo.name IN ('apache/spark', 'prakhar1989/awesome-courses'))
)) AND (v:repo.name NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
GROUP BY repo_name
ORDER BY stars DESC, repo_name 
LIMIT 50
