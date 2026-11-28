package dbos

import (
	"cmp"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

type (
	aggregateJournal struct {
		mu              sync.Mutex
		id              timebox.AggregateID
		key             string
		stream          *stream
		archives        []string
		archiveSnapshot []string
		revision        uint64
	}

	journalView struct {
		projection
		logs []*aggregateJournal
	}
)

// Each first record also identifies the aggregate for type-wide index queries.
func (b *Backend) journal(id timebox.AggregateID) *aggregateJournal {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if j := b.logs[id]; j != nil {
		return j
	}
	j := &aggregateJournal{
		id:  id,
		key: encodeID(id.Type) + "/" + encodeID(id.Key),
	}
	b.logs[id] = j
	return j
}

func (b *Backend) lockJournals(ids []timebox.AggregateID) *journalView {
	ids = slices.Clone(ids)
	slices.SortFunc(ids, func(a, b timebox.AggregateID) int {
		if order := cmp.Compare(a.Type, b.Type); order != 0 {
			return order
		}
		return cmp.Compare(a.Key, b.Key)
	})
	v := &journalView{streams: map[timebox.AggregateID]*stream{}}
	for _, id := range slices.Compact(ids) {
		j := b.journal(id)
		j.mu.Lock()
		v.logs = append(v.logs, j)
		v.streams[id] = j.stream
		if id == timebox.ArchiveQueueID {
			v.archives = j.archives
			v.archiveSnapshot = j.archiveSnapshot
		}
	}
	return v
}

func (v *journalView) unlock() {
	for _, j := range v.logs {
		j.mu.Unlock()
	}
}

func (b *Backend) refresh(v *journalView) error {
	if err := b.checkOpen(); err != nil || len(v.logs) == 0 {
		return err
	}
	for {
		var ids []string
		for _, j := range v.logs {
			for i := range journalPageSize {
				ids = append(ids, j.recordID(j.revision+uint64(i)+1))
			}
		}
		items, err := b.loadWorkflowInputs(ids)
		if err != nil {
			return err
		}
		full := false
		for n, j := range v.logs {
			page := ids[n*journalPageSize:][:journalPageSize]
			count := 0
			for i, id := range page {
				input, ok := items[id]
				if !ok {
					continue
				}
				if i != count {
					return fmt.Errorf("%w: journal gap before %s", timebox.ErrUnexpectedResult, id)
				}
				if err := v.replay(j, input); err != nil {
					return err
				}
				count++
			}
			full = full || count == journalPageSize
		}
		if !full {
			return nil
		}
	}
}

func (v *journalView) replay(j *aggregateJournal, input any) error {
	var e journalEntry
	if err := decodeWorkflowInput(input, &e); err != nil {
		return err
	}
	return v.applyRecord(j, &e)
}

func (v *journalView) applyRecord(j *aggregateJournal, e *journalEntry) error {
	if e.Mutation == nil || e.Revision != j.revision+1 {
		return fmt.Errorf("%w: journal revision for %s", timebox.ErrUnexpectedResult, j.id)
	}
	switch {
	case e.Mutation.Archive == nil:
		v.apply(e)
	case j.id == timebox.ArchiveQueueID:
		v.addArchive(e.ArchiveID)
	default:
		delete(v.streams, j.id)
	}
	j.stream = v.streams[j.id]
	j.revision = e.Revision
	if j.id == timebox.ArchiveQueueID {
		j.archives = v.archives
		j.archiveSnapshot = v.archiveSnapshot
	}
	return nil
}

func (b *Backend) enqueueJournal(
	tx pgx.Tx, v *journalView, entry *journalEntry,
) ([]journalEntry, error) {
	input := entry.Mutation
	res := make([]journalEntry, 0, len(v.logs))
	records := make(map[timebox.AggregateID][]appendRecord, len(input.Appends))
	for i := range input.Appends {
		records[input.Appends[i].ID] = input.Appends[i : i+1]
	}
	for _, j := range v.logs {
		e := *entry
		if req, ok := records[j.id]; ok {
			e.Mutation = &mutation{Appends: req}
		}
		e.Revision = j.revision + 1
		_, err := dbos.Enqueue[timebox.Empty](b.runtime, journalQueueName, journalWorkflowName, e,
			dbos.WithEnqueueWorkflowID(j.recordID(e.Revision)),
			dbos.WithEnqueueWorkflowIDReusePolicy(dbos.WorkflowIDReusePolicyReject),
			dbos.WithEnqueueTransaction(tx))
		if err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, nil
}

func (b *Backend) aggregateIDs(typ timebox.ID) ([]timebox.AggregateID, error) {
	prefix := "first/" + encodeID(typ) + "/"
	items, err := dbos.ListWorkflows(b.runtime, dbos.WithFilterWorkflowIDPrefix(prefix),
		dbos.WithFilterLoadInput(false), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return nil, err
	}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	var ids []timebox.AggregateID
	for id := range b.logs {
		if id.Type == typ {
			ids = append(ids, id)
		}
	}
	for _, item := range items {
		key, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(item.ID, prefix))
		if err != nil {
			return nil, err
		}
		ids = append(ids, timebox.NewAggregateID(typ, timebox.ID(key)))
	}
	return ids, nil
}

func (j *aggregateJournal) recordID(revision uint64) string {
	if revision == 1 {
		return "first/" + j.key
	}
	return "record/" + j.key + "/" + strconv.FormatUint(revision, 10)
}

func encodeID(id timebox.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}
