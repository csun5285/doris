SELECT payload:issue.user FROM github_events WHERE payload:issue.state = "open" and  cast(payload:issue.locked as int) = 0 order by cast(repo:id as int), id limit 10;
