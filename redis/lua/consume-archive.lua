-- Atomically acknowledge and delete a stream entry
-- KEYS[1] = stream key
-- ARGV[1] = consumer group
-- ARGV[2] = stream entry ID
-- Returns: {ackCount, delCount}

local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
local deleted = redis.call('XDEL', KEYS[1], ARGV[2])
return {acked, deleted}
