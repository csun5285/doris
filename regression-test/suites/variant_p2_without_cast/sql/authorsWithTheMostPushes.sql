 SELECT
      actor:login ,
      count() AS c,
      count(distinct repo:name) AS repos
  FROM github_events
  WHERE type = 'PushEvent'
  GROUP BY actor:login 
  ORDER BY c DESC, 1, 3
  LIMIT 50
