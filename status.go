package timebox

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var removeAggregateFromStatusScript = redis.NewScript(`
	local currentStatus = redis.call('HGET', KEYS[1], ARGV[2]) or ""
	if currentStatus == ARGV[1] then
		redis.call('HDEL', KEYS[1], ARGV[2])
	end
	return redis.call('SREM', KEYS[2], ARGV[2])
`)

// ListAggregatesByStatus returns the aggregate IDs currently indexed under the
// provided status
func (s *Store) ListAggregatesByStatus(
	ctx context.Context, status string,
) ([]AggregateID, error) {
	key := s.buildStatusSetKey(status)
	members, err := s.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	res := make([]AggregateID, 0, len(members))
	for _, member := range members {
		res = append(res, ParseAggregateID(member, ":"))
	}
	return res, nil
}

// RemoveAggregateFromStatus manually removes an aggregate from the provided
// status index. If the aggregate's current status still matches, that cached
// status is cleared as well
func (s *Store) RemoveAggregateFromStatus(
	ctx context.Context, id AggregateID, status string,
) error {
	keys := []string{
		s.buildStatusHashKey(),
		s.buildStatusSetKey(status),
	}
	args := []any{status, id.Join(":")}
	_, err := removeAggregateFromStatusScript.Run(
		ctx, s.client, keys, args...,
	).Result()
	return err
}

func (s *Store) buildStatusSetKey(status string) string {
	return s.buildGlobalKey(statusSuffix + ":" + status)
}

func (s *Store) buildStatusHashKey() string {
	return s.buildGlobalKey(statusSuffix)
}
