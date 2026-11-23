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
	r.aggType = ARGV[ai]; ai = ai + 1
	r.status = flags % 2 == 1
	r.tags = math.floor(flags / 2) % 2 == 1
	local delete = math.floor(flags / 4) % 2 == 1

	r.eventsKey = ki; ki = ki + 1
	r.baseKey = ki; ki = ki + 1
	if r.status then
		r.statusKey = ki; ki = ki + 1
		r.statusTypeKey = ki; ki = ki + 1
		r.newStatus = ARGV[ai]; ai = ai + 1
		r.statusAt = ARGV[ai]; ai = ai + 1
	end
	if r.tags then
		r.tagStateKey = ki; ki = ki + 1
		r.tagCount = tonumber(ARGV[ai]); ai = ai + 1
		r.firstTag = ai; ai = ai + r.tagCount * 2
	end
	r.firstEvent = ai; ai = ai + r.count

	if delete then
		-- status hash, tag state, snapshot value, snapshot sequence,
		-- status type root
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
		-- Hash values and set members both lead with the 16-char status time
		local typed = KEYS[r.statusTypeKey] .. ":" .. r.aggType .. ":"
		local old = redis.call('HGET', KEYS[r.statusKey], r.aggID)
		if old then
			redis.call(
				'ZREM', typed .. string.sub(old, 17),
				string.sub(old, 1, 16) .. r.aggID
			)
		end
		if r.newStatus ~= "" then
			redis.call(
				'HSET', KEYS[r.statusKey], r.aggID, r.statusAt .. r.newStatus
			)
			redis.call(
				'ZADD', typed .. r.newStatus, 0, r.statusAt .. r.aggID
			)
		else
			redis.call('HDEL', KEYS[r.statusKey], r.aggID)
		end
	end

	if r.tags then
		for j = 0, r.tagCount - 1 do
			local a = r.firstTag + (j * 2)
			if ARGV[a + 1] == "1" then
				redis.call('SADD', KEYS[r.tagStateKey], ARGV[a])
			else
				redis.call('SREM', KEYS[r.tagStateKey], ARGV[a])
			end
		end
	end
end

-- Runs after the writes above, so it also removes what a delete just stored
for i = 1, reqCount do
	local r = reqs[i]
	if r.deleteKey then
		local k = r.deleteKey
		local status = redis.call('HGET', KEYS[k], r.aggID)
		if status then
			redis.call(
				'ZREM',
				KEYS[k + 4] .. ":" .. r.aggType .. ":" .. string.sub(status, 17),
				string.sub(status, 1, 16) .. r.aggID
			)
			redis.call('HDEL', KEYS[k], r.aggID)
		end
		local seq = r.atSeq + r.count
		redis.call('DEL', KEYS[r.eventsKey], KEYS[k + 1], KEYS[k + 2])
		redis.call('SET', KEYS[k + 3], seq)
		redis.call('SET', KEYS[r.baseKey], seq)
	end
end

return {1, 0, {}, 0}
