SELECT count(cast(payload:issue as string)) FROM github_events where payload:issue.state = "closed";