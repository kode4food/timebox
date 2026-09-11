SELECT snapshot_data, snapshot_seq
FROM timebox_snapshots
WHERE aggregate_key = $1
