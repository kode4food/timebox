-- Atomically save snapshot only if new sequence is greater than stored,
-- trimming the events it covers when asked
-- KEYS[1] = snapshot key
-- KEYS[2] = snapshot sequence key
-- KEYS[3] = event list key
-- KEYS[4] = base sequence key
-- ARGV[1] = snapshot data
-- ARGV[2] = snapshot sequence
-- ARGV[3] = "1" to trim events

local newSeq = tonumber(ARGV[2])
local storedSeqStr = redis.call('GET', KEYS[2])

if storedSeqStr then
	local storedSeq = tonumber(storedSeqStr)
	if newSeq <= storedSeq then
		return 1
	end
end

if ARGV[3] == "1" then
	local base = tonumber(redis.call('GET', KEYS[4]) or "0")
	local dropCount = math.min(newSeq - base, redis.call('LLEN', KEYS[3]))
	if dropCount > 0 then
		redis.call('LTRIM', KEYS[3], tostring(dropCount), -1)
		redis.call('SET', KEYS[4], base + dropCount)
	end
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], newSeq)
return 1
