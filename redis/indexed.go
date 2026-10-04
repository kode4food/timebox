package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

func (b *Backend) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	aggID := joinAggregateID(id)
	status, err := b.client.HGet(
		context.Background(), b.buildStatusHashKey(), aggID,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

func (b *Backend) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	key := b.buildStatusIndexKey(status)
	members, err := b.client.ZRangeWithScores(
		context.Background(), key, 0, -1,
	).Result()
	if err != nil {
		return nil, err
	}

	res := make([]timebox.StatusEntry, 0, len(members))
	for _, member := range members {
		res = append(res, timebox.StatusEntry{
			ID:        parseAggregateID(fmt.Sprint(member.Member)),
			Timestamp: time.UnixMilli(int64(member.Score)).UTC(),
		})
	}
	return res, nil
}

// ListAggregatesByStatusPrefix lists matching aggregates for a status
func (b *Backend) ListAggregatesByStatusPrefix(
	req timebox.StatusPrefixRequest,
) ([]timebox.AggregateID, error) {
	key := b.buildStatusIndexKey(req.Status)
	prefix := timebox.NewAggregateID(req.Type, req.Prefix)
	pattern := escapeScanPattern(joinAggregateID(prefix)) + "*"
	var cursor uint64
	var res []timebox.AggregateID
	for {
		members, next, err := b.client.ZScan(
			context.Background(), key, cursor, pattern, 128,
		).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(members); i += 2 {
			res = append(res, parseAggregateID(members[i]))
		}
		if next == 0 {
			return res, nil
		}
		cursor = next
	}
}

func (b *Backend) ListAggregatesByTag(
	tag string,
) ([]timebox.AggregateID, error) {
	members, err := b.client.SMembers(
		context.Background(), b.buildTagIndexKey(tag),
	).Result()
	if err != nil {
		return nil, err
	}

	ids := make([]timebox.AggregateID, 0, len(members))
	for _, member := range members {
		ids = append(ids, parseAggregateID(member))
	}
	return ids, nil
}

func (b *Backend) buildStatusHashKey() string {
	return fmt.Sprintf("%s:%s", b.prefix, statusSuffix)
}

func (b *Backend) buildStatusIndexKey(status string) string {
	return fmt.Sprintf("%s:%s:%s", b.prefix, statusSuffix, status)
}

func (b *Backend) buildTagIndexKey(tag string) string {
	return fmt.Sprintf("%s:%s:%s", b.prefix, tagSuffix, escapeKeyPart(tag))
}

func escapeScanPattern(value string) string {
	return strings.NewReplacer(
		`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`,
	).Replace(value)
}
