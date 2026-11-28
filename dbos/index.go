package dbos

import (
	"maps"
	"slices"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus loads one aggregate's current status
func (b *Backend) GetAggregateStatus(id timebox.AggregateID) (string, error) {
	v := b.lockJournals([]timebox.AggregateID{id})
	defer v.unlock()
	if err := b.refresh(v); err != nil {
		return "", err
	}
	if s := v.streams[id]; s != nil {
		return s.status, nil
	}
	return "", nil
}

// ListAggregatesByStatus pages through an aggregate type's status index
func (b *Backend) ListAggregatesByStatus(q timebox.StatusQuery) ([]timebox.StatusEntry, error) {
	if q.Limit <= 0 {
		return nil, timebox.ErrInvalidLimit
	}
	if err := b.checkOpen(); err != nil {
		return nil, err
	}
	ids, err := b.aggregateIDs(q.Type)
	if err != nil {
		return nil, err
	}
	v := b.lockJournals(ids)
	defer v.unlock()
	if err := b.refresh(v); err != nil {
		return nil, err
	}

	var res []timebox.StatusEntry
	for id, s := range v.streams {
		if s == nil || q.Status == "" || s.status != q.Status || id.Type != q.Type {
			continue
		}
		entry := timebox.StatusEntry{
			ID:        id,
			Timestamp: s.statusAt,
		}
		if !q.Through.IsZero() && entry.Timestamp.After(q.Through) {
			continue
		}
		if q.After != nil && compareStatuses(entry, *q.After) >= 0 {
			continue
		}
		res = append(res, entry)
	}
	slices.SortFunc(res, func(l, r timebox.StatusEntry) int {
		return compareStatuses(r, l)
	})
	return res[:min(len(res), q.Limit)], nil
}

// GetAggregateTags loads the current tag memberships of the supplied aggregates
func (b *Backend) GetAggregateTags(
	ids ...timebox.AggregateID,
) (map[timebox.AggregateID][]string, error) {
	v := b.lockJournals(ids)
	defer v.unlock()
	if err := b.refresh(v); err != nil {
		return nil, err
	}

	res := map[timebox.AggregateID][]string{}
	for _, id := range ids {
		if s := v.streams[id]; s != nil && len(s.tags) != 0 {
			res[id] = slices.Sorted(maps.Keys(s.tags))
		}
	}
	return res, nil
}

func compareStatuses(l, r timebox.StatusEntry) int {
	if cmp := l.Timestamp.Compare(r.Timestamp); cmp != 0 {
		return cmp
	}
	switch {
	case l.ID.Key < r.ID.Key:
		return -1
	case l.ID.Key > r.ID.Key:
		return 1
	default:
		return 0
	}
}
