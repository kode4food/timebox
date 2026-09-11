INSERT INTO timebox_snapshots (
	aggregate_key, base_seq,
	snapshot_seq, snapshot_data
) VALUES ($1, $2, $3, $4)
ON CONFLICT (aggregate_key) DO UPDATE
SET base_seq = EXCLUDED.base_seq,
    snapshot_seq = EXCLUDED.snapshot_seq,
    snapshot_data = EXCLUDED.snapshot_data
