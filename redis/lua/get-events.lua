-- Get events from list starting at a given sequence
-- KEYS[1] = event list key
-- ARGV[1] = starting sequence (0-based)

local fromSeq = tonumber(ARGV[1])
return redis.call('LRANGE', KEYS[1], fromSeq, -1)
