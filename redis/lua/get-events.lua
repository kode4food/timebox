-- Get events from list starting at a given sequence
-- KEYS[1] = event list key
-- KEYS[2] = base sequence key
-- ARGV[1] = starting sequence (0-based)
-- Returns: {base, events}

local fromSeq = tonumber(ARGV[1])
local base = tonumber(redis.call('GET', KEYS[2]) or "0")
local startIndex = fromSeq - base
if startIndex < 0 then
	startIndex = 0
end
local events = redis.call('LRANGE', KEYS[1], startIndex, -1)
return {base, events}
