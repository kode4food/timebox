package timebox

import (
	"context"
	"sort"
)

// ListAggregatesByLabel returns the aggregates indexed under a label/value
// pair. Label indexing is append-only
func (s *Store) ListAggregatesByLabel(
	ctx context.Context, label, value string) ([]AggregateID, error) {
	members, err := s.client.SMembers(
		ctx, s.buildLabelIndexKey(label, value),
	).Result()
	if err != nil {
		return nil, err
	}

	ids := make([]AggregateID, 0, len(members))
	for _, member := range members {
		ids = append(ids, ParseAggregateID(member, ":"))
	}
	return ids, nil
}

// ListLabelValues returns the unique values indexed for a label
func (s *Store) ListLabelValues(
	ctx context.Context, label string,
) ([]string, error) {
	vals, err := s.client.SMembers(ctx, s.buildLabelValuesKey(label)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(vals)
	return vals, nil
}
