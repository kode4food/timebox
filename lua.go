package timebox

const (
	luaAppendEvents = `
		-- Atomically append events to list with sequence consistency check
		-- KEYS[1] = event list key
		-- ARGV[1] = expected sequence (current list length)
		-- ARGV[2..N] = event data (JSON)

		local currentLen = redis.call('LLEN', KEYS[1])
		local expected = tonumber(ARGV[1])

		if expected ~= currentLen then
			if expected < currentLen then
				local newEvents = redis.call('LRANGE', KEYS[1], expected, -1)
				local result = {0, currentLen}
				for i = 1, #newEvents do
					result[i + 2] = newEvents[i]
				end
				return result
			end
			return {0, currentLen}
		end

		for i = 2, #ARGV do
			redis.call('RPUSH', KEYS[1], ARGV[i])
		end

		return {1, redis.call('LLEN', KEYS[1])}
		`

	luaGetEvents = `
		-- Get events from list starting at a given sequence
		-- KEYS[1] = event list key
		-- ARGV[1] = starting sequence (0-based)

		local fromSeq = tonumber(ARGV[1])
		return redis.call('LRANGE', KEYS[1], fromSeq, -1)
		`

	luaPutSnapshot = `
		-- Atomically save snapshot only if new sequence is greater than stored
		-- KEYS[1] = snapshot key
		-- KEYS[2] = snapshot sequence key
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

		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('SET', KEYS[2], newSeq)
		return 1
		`

	luaGetSnapshot = `
		-- Atomically get snapshot and events after snapshot sequence
		-- KEYS[1] = snapshot key
		-- KEYS[2] = snapshot sequence key
		-- KEYS[3] = event list key
		-- Returns: {snapshot_data, snapshot_seq, events...}

		local snapData = redis.call('GET', KEYS[1])
		local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")

		local result = {snapData or "", snapSeq}

		local events = redis.call('LRANGE', KEYS[3], snapSeq, -1)
		for i = 1, #events do
			result[i + 2] = events[i]
		end

		return result
		`
)
