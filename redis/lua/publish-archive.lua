-- Atomically move snapshot + events to a stream
-- KEYS[1] = snapshot key
-- KEYS[2] = snapshot sequence key
-- KEYS[3] = event list key
-- KEYS[4] = stream key
-- KEYS[5] = status hash key
-- KEYS[6] = aggregate tag set key
-- KEYS[7] = tag root key
-- ARGV[1] = aggregate id string
-- Returns: {1, streamId} on success, {0} if nothing to move

local snapData = redis.call('GET', KEYS[1]) or ""
local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
local allEvents = redis.call('LRANGE', KEYS[3], 0, -1)
local status = redis.call('HGET', KEYS[5], ARGV[1]) or ""
local tags = redis.call('SMEMBERS', KEYS[6])

if snapData == ""
	and #allEvents == 0
	and status == ""
	and #tags == 0
then
	return {0}
end

local payload = cjson.encode({
	id = ARGV[1],
	snap = snapData,
	seq = snapSeq,
	events = allEvents,
})

local streamId = redis.call('XADD', KEYS[4], '*', 'payload', payload)
if status ~= "" then
	redis.call('ZREM', KEYS[5] .. ":" .. status, ARGV[1])
	redis.call('HDEL', KEYS[5], ARGV[1])
end
for i = 1, #tags do
	local tag = tags[i]
	local memberKey = KEYS[7] .. ":" .. tag
	redis.call('SREM', memberKey, ARGV[1])
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[6])
return {1, streamId}
