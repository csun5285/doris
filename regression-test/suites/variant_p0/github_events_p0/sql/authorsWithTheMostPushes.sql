SELECT
      v:actor.login,
      count() AS c,
      count(distinct v:repo.name) AS repos
  FROM github_events
  WHERE v:type = 'PushEvent'
  GROUP BY v:actor.login
  ORDER BY c DESC, 1, 3
  LIMIT 50
