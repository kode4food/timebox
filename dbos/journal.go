package dbos

import (
	"encoding/json"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/kode4food/timebox"
)

type journalEntry struct {
	Mutation  *mutation
	ArchiveID string
	Revision  uint64
}

const (
	journalQueueName    = "timebox-journal"
	journalWorkflowName = "timebox-record"
	journalPageSize     = 32
)

func (b *Backend) loadLegacyJournal() error {
	state := projection{streams: map[timebox.AggregateID]*stream{}}
	var revision uint64
	for {
		start := revision + 1
		ids := make([]string, journalPageSize)
		for i := range ids {
			ids[i] = b.journalID(start + uint64(i))
		}
		items, err := b.loadWorkflowInputs(ids)
		if err != nil {
			return err
		}
		for i, id := range ids[:len(items)] {
			var entry journalEntry
			if err := decodeWorkflowInput(items[id], &entry); err != nil {
				return fmt.Errorf("load journal %s: %w", id, err)
			}
			if entry.Mutation == nil || entry.Revision != start+uint64(i) {
				return fmt.Errorf("%w: invalid journal entry %s", timebox.ErrUnexpectedResult, id)
			}
			state.apply(&entry)
			revision = entry.Revision
		}
		if len(items) < journalPageSize {
			break
		}
	}
	for id, s := range state.streams {
		j := b.journal(id)
		j.stream = s
		if id == timebox.ArchiveQueueID {
			j.archives = state.archives
			j.archiveSnapshot = state.archiveSnapshot
		}
	}
	return nil
}

func (b *Backend) journalID(rev uint64) string {
	return fmt.Sprintf("%s-journal-%d", b.cfg.Namespace, rev)
}

func (b *Backend) loadWorkflowInputs(ids []string) (map[string]any, error) {
	items, err := dbos.ListWorkflows(b.runtime, dbos.WithFilterWorkflowIDs(ids...),
		dbos.WithFilterLoadInput(true), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return nil, err
	}
	res := make(map[string]any, len(items))
	for _, item := range items {
		res[item.ID] = item.Input
	}
	return res, nil
}

func journalWorkflow(_ dbos.Context, _ journalEntry) (timebox.Empty, error) {
	return timebox.Empty{}, nil
}

func decodeWorkflowInput(input any, dst any) error {
	raw, ok := input.(string)
	if !ok {
		return fmt.Errorf("%w: missing DBOS workflow input", timebox.ErrUnexpectedResult)
	}
	return json.Unmarshal([]byte(raw), dst)
}
