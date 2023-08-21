set enable_two_phase_read_opt = false;
SELECT v:repo.name FROM github_events WHERE v:type = 'WatchEvent' ORDER BY v:created_at LIMIT 50