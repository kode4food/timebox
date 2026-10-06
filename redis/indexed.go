package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

// ListAggregatesByStatus lists aggregates matching the query. Type and
// KeyPrefix narrow by aggregate-id prefix; Through bounds by status time,
// each via its own native redis op
func (b *Backend) ListAggregatesByStatus(
	q timebox.StatusQuery,
) ([]timebox.StatusEntry, error) {
	key := b.buildStatusIndexKey(q.Status)

	var prefix string
	if q.Type != "" || q.KeyPrefix != "" {
		prefix = joinAggregateID(timebox.NewAggregateID(q.Type, q.KeyPrefix))
	}

	if !q.Through.IsZero() {
		members, err := b.client.ZRangeByScoreWithScores(
			context.Background(), key, &redis.ZRangeBy{
				Min: "-inf",
				Max: strconv.FormatInt(q.Through.UnixMilli(), 10),
			},
		).Result()
		if err != nil {
			return nil, err
		}
		// no native op scores and pattern-matches in one call, so a
		// concurrent prefix filters the score-bounded result in-process
		return statusEntriesFromZ(members, prefix), nil
	}

	if prefix != "" {
		return b.scanStatusPrefix(key, prefix)
	}

	members, err := b.client.ZRangeWithScores(
		context.Background(), key, 0, -1,
	).Result()
	if err != nil {
		return nil, err
	}
	return statusEntriesFromZ(members, ""), nil
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

func (b *Backend) scanStatusPrefix(
	key, prefix string,
) ([]timebox.StatusEntry, error) {
	pattern := escapeScanPattern(prefix) + "*"
	var cursor uint64
	var res []timebox.StatusEntry
	for {
		members, next, err := b.client.ZScan(
			context.Background(), key, cursor, pattern, 128,
		).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i+1 < len(members); i += 2 {
			score, err := strconv.ParseFloat(members[i+1], 64)
			if err != nil {
				return nil, err
			}
			res = append(res, timebox.StatusEntry{
				ID:        parseAggregateID(members[i]),
				Timestamp: time.UnixMilli(int64(score)).UTC(),
			})
		}
		if next == 0 {
			return res, nil
		}
		cursor = next
	}
}

func statusEntriesFromZ(
	members []redis.Z, prefix string,
) []timebox.StatusEntry {
	res := make([]timebox.StatusEntry, 0, len(members))
	for _, member := range members {
		m := fmt.Sprint(member.Member)
		if prefix != "" && !strings.HasPrefix(m, prefix) {
			continue
		}
		res = append(res, timebox.StatusEntry{
			ID:        parseAggregateID(m),
			Timestamp: time.UnixMilli(int64(member.Score)).UTC(),
		})
	}
	return res
}

func escapeScanPattern(value string) string {
	return strings.NewReplacer(
		`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`,
	).Replace(value)
}
