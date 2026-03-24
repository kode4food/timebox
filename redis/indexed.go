package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	aggID := joinAggregateID(id)
	status, err := p.client.HGet(
		context.Background(), p.buildStatusHashKey(), aggID,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return status, nil
}

func (p *Persistence) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	key := p.buildStatusIndexKey(status)
	members, err := p.client.ZRangeWithScores(
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

func (p *Persistence) ListAggregatesByLabel(
	label, value string,
) ([]timebox.AggregateID, error) {
	members, err := p.client.SMembers(
		context.Background(), p.buildLabelIndexKey(label, value),
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

func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	vals, err := p.client.SMembers(
		context.Background(), p.buildLabelValuesKey(label),
	).Result()
	if err != nil {
		return nil, err
	}
	for i, val := range vals {
		vals[i] = unescapeKeyPart(val)
	}
	sort.Strings(vals)
	return vals, nil
}

func (p *Persistence) buildStatusHashKey() string {
	return fmt.Sprintf("%s:%s", p.prefix, statusSuffix)
}

func (p *Persistence) buildStatusIndexKey(status string) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, statusSuffix, status)
}

func (p *Persistence) buildLabelValuesKey(label string) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, labelSuffix, escapeKeyPart(label))
}

func (p *Persistence) buildLabelIndexKey(label, value string) string {
	return fmt.Sprintf("%s:%s:%s:%s",
		p.prefix, labelSuffix, escapeKeyPart(label), escapeKeyPart(value),
	)
}
