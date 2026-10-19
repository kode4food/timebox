local chunkSize = 128
local reqCount = tonumber(ARGV[1])
local ai = 2
local ki = 1
local reqs = {}

for i = 1, reqCount do
	local r = {}
	local flags = tonumber(ARGV[ai]); ai = ai + 1
	r.atSeq = tonumber(ARGV[ai]); ai = ai + 1
	r.count = tonumber(ARGV[ai]); ai = ai + 1
	r.aggID = ARGV[ai]; ai = ai + 1
	r.status = flags % 2 == 1
	r.tags = math.floor(flags / 2) % 2 == 1
	local delete = math.floor(flags / 4) % 2 == 1

	r.eventsKey = ki; ki = ki + 1
	r.baseKey = ki; ki = ki + 1
	if r.status then
		r.statusKey = ki; ki = ki + 1
		r.newStatus = ARGV[ai]; ai = ai + 1
		r.statusAt = ARGV[ai]; ai = ai + 1
	end
	if r.tags then
		r.tagStateKey = ki; ki = ki + 1
		r.tagRootKey = ki; ki = ki + 1
		r.tagCount = tonumber(ARGV[ai]); ai = ai + 1
		r.firstTag = ai; ai = ai + r.tagCount * 2
	end
	r.firstEvent = ai; ai = ai + r.count

	if delete then
		-- status hash, tag state, tag root, snapshot value, snapshot sequence
		r.deleteKey = ki; ki = ki + 5
	end

	local base = tonumber(redis.call('GET', KEYS[r.baseKey]) or "0")
	local seq = base + redis.call('LLEN', KEYS[r.eventsKey])
	if r.atSeq ~= seq then
		local from = r.atSeq - base
		if r.atSeq < seq and from >= 0 then
			local evs = redis.call(
				'LRANGE', KEYS[r.eventsKey], from, -1
			)
			return {0, seq, evs, i}
		end
		return {0, seq, {}, i}
	end
	reqs[i] = r
end

for i = 1, reqCount do
	local r = reqs[i]
	local idx = r.firstEvent
	local last = r.firstEvent + r.count - 1
	while idx <= last do
		local stop = math.min(idx + chunkSize - 1, last)
		local chunk = {}
		for j = idx, stop do
			table.insert(chunk, ARGV[j])
		end
		redis.call('RPUSH', KEYS[r.eventsKey], unpack(chunk))
		idx = stop + 1
	end

	if r.status then
		local prefix = KEYS[r.statusKey] .. ":"
		local old = redis.call(
			'HGET', KEYS[r.statusKey], r.aggID
		) or ""
		if old ~= "" and old ~= r.newStatus then
			redis.call('ZREM', prefix .. old, r.aggID)
		end
		if r.newStatus ~= "" then
			redis.call(
				'HSET', KEYS[r.statusKey], r.aggID, r.newStatus
			)
			redis.call(
				'ZADD', prefix .. r.newStatus, r.statusAt, r.aggID
			)
		else
			redis.call('HDEL', KEYS[r.statusKey], r.aggID)
		end
	end

	if r.tags then
		for j = 0, r.tagCount - 1 do
			local a = r.firstTag + (j * 2)
			local tag = ARGV[a]
			local member = KEYS[r.tagRootKey] .. ":" .. tag
			if ARGV[a + 1] == "1" then
				redis.call('SADD', KEYS[r.tagStateKey], tag)
				redis.call('SADD', member, r.aggID)
			else
				redis.call('SREM', KEYS[r.tagStateKey], tag)
				redis.call('SREM', member, r.aggID)
			end
		end
	end
end

-- Runs after the writes above, so it also removes what a delete just stored
for i = 1, reqCount do
	local r = reqs[i]
	if r.deleteKey then
		local k = r.deleteKey
		local status = redis.call('HGET', KEYS[k], r.aggID) or ""
		if status ~= "" then
			redis.call('ZREM', KEYS[k] .. ":" .. status, r.aggID)
			redis.call('HDEL', KEYS[k], r.aggID)
		end
		local tags = redis.call('SMEMBERS', KEYS[k + 1])
		for j = 1, #tags do
			redis.call('SREM', KEYS[k + 2] .. ":" .. tags[j], r.aggID)
		end
		local seq = r.atSeq + r.count
		redis.call('DEL', KEYS[r.eventsKey], KEYS[k + 1], KEYS[k + 3])
		redis.call('SET', KEYS[k + 4], seq)
		redis.call('SET', KEYS[r.baseKey], seq)
	end
end

return {1, 0, {}, 0}
