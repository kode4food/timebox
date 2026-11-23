SELECT aggregate_key, tag
FROM timebox_tags
WHERE aggregate_key = ANY($1)
