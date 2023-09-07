set enable_two_phase_read_opt = false;
SELECT cast(repo:name as string) FROM github_events WHERE type = 'WatchEvent' ORDER BY created_at LIMIT 50