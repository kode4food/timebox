package timebox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

type (
	// Index stores optional projection metadata derived from an event
	Index struct {
		// Status represents the resultant aggregate status. nil means no
		// status change, and "" clears any prior status
		Status *string `json:"status,omitempty"`

		// Labels updates current label values for the aggregate. nil means no
		// label changes, and empty values remove the label
		Labels map[string]string `json:"labels,omitempty"`
	}

	// Indexer derives projection metadata for an event batch
	Indexer func([]*Event) []*Index

	// StatusEntry holds an aggregate ID and the time it entered a status
	StatusEntry struct {
		ID        AggregateID
		Timestamp time.Time
	}
)

// GetAggregateStatus returns the current indexed status for an aggregate. If
// the aggregate has no current status, it returns ""
func (s *Store) GetAggregateStatus(
	ctx context.Context, id AggregateID,
) (string, error) {
	aggID := id.Join(":")
	status, err := s.client.HGet(ctx, s.buildStatusHashKey(), aggID).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

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

// ListAggregatesByLabel returns the aggregates currently indexed under a
// label/value pair
func (s *Store) ListAggregatesByLabel(
	ctx context.Context, label, value string,
) ([]AggregateID, error) {
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

// ListLabelValues returns the unique current values indexed for a label
func (s *Store) ListLabelValues(
	ctx context.Context, label string,
) ([]string, error) {
	vals, err := s.client.SMembers(ctx, s.buildLabelValuesKey(label)).Result()
	if err != nil {
		return nil, err
	}
	for i, val := range vals {
		vals[i] = unescapeKeyPart(val)
	}
	sort.Strings(vals)
	return vals, nil
}

func (s *Store) buildStatusIndexKey(status string) string {
	return s.buildGlobalKey(statusSuffix + ":" + status)
}

func (s *Store) buildStatusHashKey() string {
	return s.buildGlobalKey(statusSuffix)
}

func (s *Store) buildLabelValuesKey(label string) string {
	return s.buildGlobalKey(
		fmt.Sprintf("%s:%s", labelSuffix, escapeKeyPart(label)),
	)
}

func (s *Store) buildLabelIndexKey(label, value string) string {
	return s.buildGlobalKey(
		fmt.Sprintf("%s:%s:%s",
			labelSuffix, escapeKeyPart(label), escapeKeyPart(value),
		),
	)
}
