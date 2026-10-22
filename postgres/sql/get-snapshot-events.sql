-- One statement reads both, so a concurrent trim cannot land between them
SELECT TRUE AS snapshot, snapshot_seq AS sequence, 0::bigint AS event_at,
	''::text AS event_type, snapshot_data AS data
FROM timebox_snapshots
WHERE aggregate_key = $1
UNION ALL
SELECT FALSE, e.sequence, e.event_at, e.event_type, e.data
FROM timebox_events e
WHERE e.aggregate_key = $1
  AND e.sequence >= COALESCE((
	SELECT s.snapshot_seq
	FROM timebox_snapshots s
	WHERE s.aggregate_key = $1
  ), 0)
ORDER BY snapshot DESC, sequence
