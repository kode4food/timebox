-- Atomically get snapshot and events after snapshot sequence
-- KEYS[1] = snapshot key
-- KEYS[2] = snapshot sequence key
-- KEYS[3] = event list key
-- Returns: {snapshot_data, snapshot_seq, newEvents}

local snapData = redis.call('GET', KEYS[1])
local snapSeq = tonumber(redis.call('GET', KEYS[2]) or "0")
local newEvents = redis.call('LRANGE', KEYS[3], 0, -1)
return {snapData or "", snapSeq, newEvents}
