SELECT COALESCE(s.base_seq, 0),
       COALESCE(s.snapshot_seq, 0),
       COALESCE((
           SELECT e.sequence + 1
           FROM timebox_events e
           WHERE e.aggregate_key = $1
           ORDER BY e.sequence DESC
           LIMIT 1
       ), COALESCE(s.base_seq, 0))
FROM timebox_statuses i
LEFT JOIN timebox_snapshots s
  ON s.aggregate_key = i.aggregate_key
WHERE i.aggregate_key = $1
FOR UPDATE OF i
