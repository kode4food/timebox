-- Atomically bundle an aggregate's snapshot and events into the shared
-- archive queue's event list, then remove the aggregate's live records
--
-- KEYS[1] = id snapshot value key
-- KEYS[2] = id snapshot sequence key
-- KEYS[3] = id events key
-- KEYS[4] = status hash key
-- KEYS[5] = id tag state key
-- KEYS[6] = archive queue events key
-- KEYS[7] = archive queue base sequence key
-- KEYS[8] = id base sequence key
-- KEYS[9] = status type root key
-- ARGV[1] = id aggregate id string, for status lookups
-- ARGV[2] = id aggregate id type
-- ARGV[3] = id aggregate id key
-- ARGV[4] = archive queue event timestamp, RFC3339Nano
-- ARGV[5] = archive queue aggregate id type
-- ARGV[6] = archive queue aggregate id key
-- ARGV[7] = id aggregate type, escaped for the status type key
-- Returns: {1} if archived, {0} if id had nothing to archive

local snapData = redis.call('GET', KEYS[1]) or ""
local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
local rawEvents = redis.call('LRANGE', KEYS[3], 0, -1)
local status = redis.call('HGET', KEYS[4], ARGV[1]) or ""
local tagCount = redis.call('SCARD', KEYS[5])

if snapData == ""
	and #rawEvents == 0
	and status == ""
	and tagCount == 0
then
	return {0}
end

local events = {}
for i = 1, #rawEvents do
	events[i] = cjson.decode(rawEvents[i])
end

local record = {
	AggregateID = {ARGV[2], ARGV[3]},
	Events = events,
	SnapshotSequence = snapSeq,
}
if snapData ~= "" then
	record.SnapshotData = cjson.decode(snapData)
end

local message = {
	type = "_tb.archive.recorded",
	aggregate_id = {ARGV[2], ARGV[3]},
	data = record,
}

local base = tonumber(redis.call('GET', KEYS[7]) or "0")
local seq = base + redis.call('LLEN', KEYS[6])
local ev = {
	timestamp = ARGV[4],
	type = "_tb.queue.added",
	aggregate_id = {ARGV[5], ARGV[6]},
	data = message,
	sequence = seq,
}
redis.call('RPUSH', KEYS[6], cjson.encode(ev))

if status ~= "" then
	-- The hash value leads with the 16-char status time the set member also leads with
	redis.call(
		'ZREM', KEYS[9] .. ":" .. ARGV[7] .. ":" .. string.sub(status, 17),
		string.sub(status, 1, 16) .. ARGV[1]
	)
	redis.call('HDEL', KEYS[4], ARGV[1])
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[5], KEYS[8])
return {1}
