SELECT
  repo:name as repo_name,
  count() AS stars
FROM github_events
WHERE (type = 'WatchEvent') AND (actor:login  IN
(
    SELECT actor:login 
    FROM github_events
    WHERE (type = 'WatchEvent') AND (repo:name IN ('apache/spark', 'prakhar1989/awesome-courses'))
)) AND (repo:name NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
GROUP BY repo_name
ORDER BY stars DESC, repo_name 
LIMIT 50
