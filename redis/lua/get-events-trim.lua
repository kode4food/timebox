-- Get events from list starting at a given sequence
-- KEYS[1] = event list key
-- KEYS[2] = snapshot sequence key
-- ARGV[1] = starting sequence (0-based)

local fromSeq = tonumber(ARGV[1])
local offset = tonumber(redis.call('GET', KEYS[2]) or "0")
local startIndex = fromSeq - offset
if startIndex < 0 then
	startIndex = 0
end
local events = redis.call('LRANGE', KEYS[1], startIndex, -1)
return {offset, events}
