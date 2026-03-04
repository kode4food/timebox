package timebox

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var removeAggregateFromStatusScript = redis.NewScript(`
	local currentStatus = redis.call('HGET', KEYS[1], ARGV[2]) or ""
	if currentStatus == ARGV[1] then
		redis.call('HDEL', KEYS[1], ARGV[2])
	end
	return redis.call('ZREM', KEYS[2], ARGV[2])
`)

// ListAggregatesByStatus returns the aggregates currently indexed under the
// provided status, including when they entered that status
func (s *Store) ListAggregatesByStatus(
	ctx context.Context, status string,
) ([]StatusEntry, error) {
	key := s.buildStatusIndexKey(status)
	members, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	res := make([]StatusEntry, 0, len(members))
	for _, member := range members {
		res = append(res, StatusEntry{
			ID:        ParseAggregateID(fmt.Sprint(member.Member), ":"),
			Timestamp: time.UnixMilli(int64(member.Score)).UTC(),
		})
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
		s.buildStatusIndexKey(status),
	}
	args := []any{status, id.Join(":")}
	_, err := removeAggregateFromStatusScript.Run(
		ctx, s.client, keys, args...,
	).Result()
	return err
}

func (s *Store) buildStatusIndexKey(status string) string {
	return s.buildGlobalKey(statusSuffix + ":" + status)
}

func (s *Store) buildStatusHashKey() string {
	return s.buildGlobalKey(statusSuffix)
}
