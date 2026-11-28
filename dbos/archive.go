package dbos

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

type (
	archivePayload struct {
		ID       timebox.AggregateID
		Events   []eventRecord
		Snapshot []byte
		Sequence int64
	}

	archiveSnapshot struct {
		Messages []*timebox.Message `json:"messages,omitempty"`
	}
)

const (
	archiveQueueName    = "timebox-archive"
	archiveWorkflowName = "timebox-archive-record"
	archiveAckTopic     = "consumed"
	archivePollInterval = 100 * time.Millisecond
)

// Archive enqueues the archive in DBOS and clears the live stream atomically
func (b *Backend) Archive(id timebox.AggregateID) error {
	return b.write(mutation{Archive: &id})
}

func (b *Backend) enqueueArchive(
	tx pgx.Tx, id timebox.AggregateID, s *stream,
) (string, error) {
	payload := archivePayload{
		ID:       id,
		Events:   s.events,
		Snapshot: s.snapshot,
		Sequence: s.snapSeq,
	}
	handle, err := dbos.Enqueue[timebox.Empty](b.runtime, archiveQueueName, archiveWorkflowName,
		payload, dbos.WithEnqueueTransaction(tx))
	if err != nil {
		return "", err
	}
	return handle.GetWorkflowID(), nil
}

func (b *Backend) ackArchives(tx pgx.Tx, refs []string, req *appendRecord) error {
	if !req.Delete {
		count := 0
		for _, ev := range req.Events {
			if ev.Type == queueItemConsumed {
				count++
			}
		}
		refs = refs[:min(count, len(refs))]
	}
	for _, id := range refs {
		if err := dbos.Send(b.runtime, id, true, archiveAckTopic,
			dbos.WithSendTransaction(tx)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) materializeEvents(
	id timebox.AggregateID, evs []eventRecord,
) ([]*timebox.Event, error) {
	if id != timebox.ArchiveQueueID || len(evs) == 0 {
		return toEvents(id, evs), nil
	}

	var refs []string
	for _, ev := range evs {
		if ev.ArchiveID != "" {
			refs = append(refs, ev.ArchiveID)
		}
	}
	inputs, err := b.archiveInputs(refs)
	if err != nil {
		return nil, err
	}
	res := make([]*timebox.Event, 0, len(evs))
	for i := range evs {
		ev := &evs[i]
		if ev.ArchiveID == "" {
			res = append(res, ev.event(id))
			continue
		}
		record, err := archiveEvent(inputs[ev.ArchiveID], ev.Sequence)
		if err != nil {
			return nil, err
		}
		res = append(res, record)
	}
	return res, nil
}

func (b *Backend) materializeArchiveSnapshot(refs []string) ([]byte, error) {
	inputs, err := b.archiveInputs(refs)
	if err != nil {
		return nil, err
	}
	snapshot := archiveSnapshot{Messages: make([]*timebox.Message, 0, len(refs))}
	for _, ref := range refs {
		ev, err := archiveEvent(inputs[ref], 0)
		if err != nil {
			return nil, err
		}
		msg, err := ev.GetValue[*timebox.Message]()
		if err != nil {
			return nil, err
		}
		snapshot.Messages = append(snapshot.Messages, msg)
	}
	return json.Marshal(snapshot)
}

func (b *Backend) archiveInputs(refs []string) (map[string]any, error) {
	result := make(map[string]any, len(refs))
	for chunk := range slices.Chunk(refs, journalPageSize) {
		inputs, err := b.loadWorkflowInputs(chunk)
		if err != nil {
			return nil, err
		}
		maps.Copy(result, inputs)
	}
	return result, nil
}

func archiveWorkflow(ctx dbos.Context, _ archivePayload) (timebox.Empty, error) {
	for {
		_, err := dbos.Recv[bool](ctx, archiveAckTopic, 24*time.Hour)
		if !errors.Is(err, dbos.ErrTimeout) {
			return timebox.Empty{}, err
		}
	}
}

func archiveEvent(input any, seq int64) (*timebox.Event, error) {
	var payload archivePayload
	if err := decodeWorkflowInput(input, &payload); err != nil {
		return nil, err
	}
	return timebox.NewArchiveQueueEvent(seq, payload.ID, &timebox.ArchiveRecord{
		AggregateID:      payload.ID,
		SnapshotData:     payload.Snapshot,
		SnapshotSequence: payload.Sequence,
		Events:           toEvents(payload.ID, payload.Events),
	})
}
