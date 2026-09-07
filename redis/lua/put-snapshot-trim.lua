-- Atomically save snapshot only if new sequence is greater than stored
-- KEYS[1] = snapshot key
-- KEYS[2] = snapshot sequence key
-- KEYS[3] = event list key
-- ARGV[1] = snapshot data
-- ARGV[2] = snapshot sequence

local newSeq = tonumber(ARGV[2])
local storedSeqStr = redis.call('GET', KEYS[2])

if storedSeqStr then
	local storedSeq = tonumber(storedSeqStr)
	if newSeq <= storedSeq then
		return 1
	end
end

local storedSeq = tonumber(storedSeqStr or "0")
local dropCount = newSeq - storedSeq
if dropCount > 0 then
	redis.call('LTRIM', KEYS[3], tostring(dropCount), -1)
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], newSeq)
return 1
