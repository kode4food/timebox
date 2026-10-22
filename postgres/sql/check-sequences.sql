-- One statement reads every sequence from one snapshot, so the checks agree
-- without a transaction or locks
SELECT timebox_current_sequence(k.key) AS actual_sequence
FROM unnest($1::text[]) WITH ORDINALITY AS k(key, ord)
ORDER BY k.ord
