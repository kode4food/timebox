package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

// statusTimeWidth is the fixed width of the hex status time that leads status hash values and
// status set members, so members sort by time, then aggregate ID
const statusTimeWidth = 16

// GetAggregateStatus loads the current indexed status for an aggregate
func (b *Backend) GetAggregateStatus(id timebox.AggregateID) (string, error) {
	aggID := joinAggregateID(id)
	val, err := b.client.HGet(context.Background(), b.buildStatusHashKey(), aggID).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val[statusTimeWidth:], nil
}

// ListAggregatesByStatus lists one page of aggregates matching the query, newest first with ties
// broken by aggregate ID. The type's status set orders members by time-prefixed ID, so each page
// is one reverse lexical range read
func (b *Backend) ListAggregatesByStatus(q timebox.StatusQuery) ([]timebox.StatusEntry, error) {
	if q.Limit <= 0 {
		return nil, timebox.ErrInvalidLimit
	}
	maxMember := "+"
	if !q.Through.IsZero() {
		maxMember = "(" + statusTime(q.Through.UnixMilli()+1)
	}
	if q.After != nil {
		maxMember = "(" + statusTime(q.After.Timestamp.UnixMilli()) + joinAggregateID(q.After.ID)
	}
	members, err := b.client.ZRangeArgs(context.Background(), redis.ZRangeArgs{
		Key:   b.buildStatusTypeKey(q.Status, q.Type),
		Start: maxMember,
		Stop:  "-",
		ByLex: true,
		Rev:   true,
		Count: int64(q.Limit),
	}).Result()
	if err != nil {
		return nil, err
	}

	res := make([]timebox.StatusEntry, 0, len(members))
	for _, m := range members {
		bits, err := strconv.ParseUint(m[:statusTimeWidth], 16, 64)
		if err != nil {
			return nil, err
		}
		res = append(res, timebox.StatusEntry{
			ID:        parseAggregateID(m[statusTimeWidth:]),
			Timestamp: time.UnixMilli(int64(bits ^ (1 << 63))).UTC(),
		})
	}
	return res, nil
}

// GetAggregateTags loads the tags each aggregate currently holds, in one pipelined round trip
func (b *Backend) GetAggregateTags(
	ids ...timebox.AggregateID,
) (map[timebox.AggregateID][]string, error) {
	cmds := make([]*redis.StringSliceCmd, len(ids))
	_, err := b.client.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		for i, id := range ids {
			cmds[i] = p.SMembers(context.Background(), b.buildTagStateKey(id))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	res := map[timebox.AggregateID][]string{}
	for i, cmd := range cmds {
		if tags := cmd.Val(); len(tags) > 0 {
			res[ids[i]] = tags
		}
	}
	return res, nil
}

func (b *Backend) buildStatusHashKey() string {
	return fmt.Sprintf("%s:%s", b.prefix, statusSuffix)
}

func (b *Backend) buildStatusTypeRootKey() string {
	return fmt.Sprintf("%s:%s", b.prefix, statusTypeSuffix)
}

// buildStatusTypeKey names one type's set for a status. The escaped type ends at the first
// unescaped separator, so the status after it stays as written
func (b *Backend) buildStatusTypeKey(status string, typ timebox.ID) string {
	return fmt.Sprintf("%s:%s:%s", b.buildStatusTypeRootKey(), escapeIDPart(string(typ)), status)
}

// statusTime flips the sign bit so negative times sort before positive ones
func statusTime(at int64) string {
	return fmt.Sprintf("%016x", uint64(at)^(1<<63))
}
