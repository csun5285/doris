SELECT payload:issue FROM github_events WHERE cast(payload:issue.state as string) = "open" order by cast(payload:push_id as int) limit 10;
