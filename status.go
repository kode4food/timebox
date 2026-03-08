package timebox

import (
	"context"
	"fmt"
	"time"
)

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

func (s *Store) buildStatusIndexKey(status string) string {
	return s.buildGlobalKey(statusSuffix + ":" + status)
}

func (s *Store) buildStatusHashKey() string {
	return s.buildGlobalKey(statusSuffix)
}
