SELECT payload:issue.user FROM github_events WHERE cast(payload:issue.state as string) = "open" and  cast(payload:issue.locked as int) = 0 order by cast(payload:push_id as int) limit 10;
