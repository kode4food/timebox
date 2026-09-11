SELECT i.aggregate_parts
FROM timebox_tags ti
JOIN timebox_statuses i
  ON i.aggregate_key = ti.aggregate_key
WHERE ti.tag = $1
