package redis

const (
	luaGetEvents = `
		-- Get events from list starting at a given sequence
		-- KEYS[1] = event list key
		-- ARGV[1] = starting sequence (0-based)

		local fromSeq = tonumber(ARGV[1])
		return redis.call('LRANGE', KEYS[1], fromSeq, -1)
		`

	luaGetEventsTrim = `
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

	luaPutSnapshotTrim = `
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
		`

	luaGetSnapshot = `
		-- Atomically get snapshot and events after snapshot sequence
		-- KEYS[1] = snapshot key
		-- KEYS[2] = snapshot sequence key
		-- KEYS[3] = event list key
		-- Returns: {snapshot_data, snapshot_seq, newEvents}

		local snapData = redis.call('GET', KEYS[1])
		local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
		local newEvents = redis.call('LRANGE', KEYS[3], snapSeq, -1)
		return {snapData or "", snapSeq, newEvents}
		`

	luaGetSnapshotTrim = `
		-- Atomically get snapshot and events after snapshot sequence
		-- KEYS[1] = snapshot key
		-- KEYS[2] = snapshot sequence key
		-- KEYS[3] = event list key
		-- Returns: {snapshot_data, snapshot_seq, newEvents}

		local snapData = redis.call('GET', KEYS[1])
		local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
		local newEvents = redis.call('LRANGE', KEYS[3], 0, -1)
		return {snapData or "", snapSeq, newEvents}
		`

	luaPublishArchive = `
		-- Atomically move snapshot + events to a stream
		-- KEYS[1] = snapshot key
		-- KEYS[2] = snapshot sequence key
		-- KEYS[3] = event list key
		-- KEYS[4] = stream key
		-- KEYS[5] = status hash key
		-- KEYS[6] = label state hash key
		-- KEYS[7] = label root key
		-- ARGV[1] = aggregate id string
		-- Returns: {1, streamId} on success, {0} if nothing to move

		local snapData = redis.call('GET', KEYS[1]) or ""
		local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
		local allEvents = redis.call('LRANGE', KEYS[3], 0, -1)
		local status = redis.call('HGET', KEYS[5], ARGV[1]) or ""
		local labels = redis.call('HGETALL', KEYS[6])

		if snapData == ""
			and #allEvents == 0
			and status == ""
			and #labels == 0
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
		for i = 1, #labels, 2 do
			local label = labels[i]
			local value = labels[i + 1]
			local valuesKey = KEYS[7] .. ":" .. label
			local memberKey = KEYS[7] .. ":" .. label .. ":" .. value
			redis.call('SREM', memberKey, ARGV[1])
			if redis.call('SCARD', memberKey) == 0 then
				redis.call('SREM', valuesKey, value)
			end
		end
		redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[6])
		return {1, streamId}
		`

	luaConsumeArchive = `
		-- Atomically acknowledge and delete a stream entry
		-- KEYS[1] = stream key
		-- ARGV[1] = consumer group
		-- ARGV[2] = stream entry ID
		-- Returns: {ackCount, delCount}

		local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
		local deleted = redis.call('XDEL', KEYS[1], ARGV[2])
		return {acked, deleted}
		`
)
