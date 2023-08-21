-- SELECT
--     v:repo.name,
--     count(distinct v:actor.login) AS u,
--     sum(star) AS stars
-- FROM
-- (
--     SELECT
--         v:repo.name,
--         CASE WHEN v:type = 'PushEvent' AND (ref LIKE '%/master' OR ref LIKE '%/main') THEN v:actor.login ELSE NULL END AS v:actor.login,
--         CASE WHEN v:type = 'WatchEvent' THEN 1 ELSE 0 END AS star
--     FROM github_events WHERE v:type IN ('PushEvent', 'WatchEvent') AND v:repo.name != '/'
-- ) t
-- GROUP BY v:repo.name
-- HAVING stars >= 100
-- ORDER BY u DESC
-- LIMIT 50
