package raft

import (
	"bytes"
	"slices"

	"github.com/kode4food/timebox"
	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	fsm struct {
		db *kvDB
	}

	// decodedEntry pairs a raft log index with its command bytes
	decodedEntry struct {
		index uint64
		cmd   Command
	}
)

func newFSM(db *kvDB) *fsm {
	return &fsm{db: db}
}

func (f *fsm) applyEntries(ents []decodedEntry) ([]*ApplyResult, error) {
	if len(ents) == 0 {
		return nil, nil
	}

	results := make([]*ApplyResult, len(ents))

	err := f.db.Update(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		lastApplied, err := loadLastAppliedTx(b)
		if err != nil {
			return err
		}

		for i, de := range ents {
			if de.index <= lastApplied {
				results[i] = &ApplyResult{}
				continue
			}

			var res *ApplyResult

			switch de.cmd.Type() {
			case CmdTypeAppend:
				req, err := de.cmd.AppendRequest()
				if err != nil {
					return err
				}
				if res, err = f.applyAppendTx(b, req); err != nil {
					return err
				}
			case CmdTypeSnapshot:
				sc, err := de.cmd.SnapshotRequest()
				if err != nil {
					return err
				}
				if res, err = f.applySnapshotTx(b, sc); err != nil {
					return err
				}
			case CmdTypeArchive:
				ac, err := de.cmd.ArchiveRequest()
				if err != nil {
					return err
				}
				if res, err = f.applyArchiveTx(b, de.index, ac); err != nil {
					return err
				}
			case CmdTypeConsumeArchive:
				ac, err := de.cmd.ConsumeArchiveRequest()
				if err != nil {
					return err
				}
				if res, err = f.applyConsumeArchiveTx(b, ac); err != nil {
					return err
				}
			default:
				return ErrCommandTypeUnknown
			}

			results[i] = res
			lastApplied = de.index
		}

		return markApplied(b, lastApplied)
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (f *fsm) applyAppendTx(
	b *kvBucket, req *timebox.AppendRequest,
) (*ApplyResult, error) {
	encodedID := encodeAggregateID(req.ID)
	meta, err := loadOrCreateMetaTx(b, encodedID)
	if err != nil {
		return nil, err
	}

	currentSeq := meta.CurrentSequence
	if req.ExpectedSequence != currentSeq {
		return conflictResultTx(b, encodedID, meta, req.ExpectedSequence)
	}

	if err := writeEventsTx(
		b, aggregateEventPrefix(encodedID), req.Events,
	); err != nil {
		return nil, err
	}
	if err := applyMutationsTx(b, meta, encodedID, req); err != nil {
		return nil, err
	}

	meta.CurrentSequence = currentSeq + int64(len(req.Events))
	if err := b.Put(
		AggregateMetaKey(encodedID), marshalMeta(meta),
	); err != nil {
		return nil, err
	}

	return &ApplyResult{Append: req}, nil
}

func (f *fsm) applySnapshotTx(
	b *kvBucket, cmd *SnapshotCommand,
) (*ApplyResult, error) {
	encodedID := encodeAggregateID(cmd.ID)
	metaKey := AggregateMetaKey(encodedID)
	snapshotKey := aggregateSnapshotKey(encodedID)
	evtPrefix := aggregateEventPrefix(encodedID)
	meta, err := loadOrCreateMetaTx(b, encodedID)
	if err != nil {
		return nil, err
	}
	if cmd.Sequence < meta.SnapshotSequence {
		return &ApplyResult{}, nil
	}

	currentSeq := meta.CurrentSequence
	if err := b.Put(snapshotKey, cmd.Data); err != nil {
		return nil, err
	}

	meta.SnapshotSequence = cmd.Sequence
	if cmd.Sequence > meta.CurrentSequence {
		meta.CurrentSequence = cmd.Sequence
	}

	if cmd.TrimEvents {
		trimTo := min(cmd.Sequence, currentSeq)
		if trimTo > meta.BaseSequence {
			for seq := meta.BaseSequence; seq < trimTo; seq++ {
				if err := b.Delete(
					aggregateEventKeyFromPrefix(evtPrefix, seq),
				); err != nil {
					return nil, err
				}
			}
			meta.BaseSequence = trimTo
		}
	}

	if err := b.Put(metaKey, marshalMeta(meta)); err != nil {
		return nil, err
	}

	return &ApplyResult{}, nil
}

func (f *fsm) applyArchiveTx(
	b *kvBucket, index uint64, cmd *ArchiveCommand,
) (*ApplyResult, error) {
	encodedID := encodeAggregateID(cmd.ID)
	meta, ok, err := loadMetaTx(b, encodedID)
	if err != nil || !ok {
		return &ApplyResult{}, err
	}

	events, err := loadEventsTx(b, encodedID, meta.BaseSequence)
	if err != nil {
		return nil, err
	}

	streamID := archiveStreamID(index)
	rec := &timebox.ArchiveRecord{
		StreamID:    streamID,
		AggregateID: cmd.ID,
		SnapshotData: append(
			[]byte(nil),
			b.Get(aggregateSnapshotKey(encodedID))...,
		),
		SnapshotSequence: meta.SnapshotSequence,
		Events:           events,
	}
	if err := putArchiveRecordTx(b, rec); err != nil {
		return nil, err
	}
	if err := deleteAggregateTx(b, encodedID, meta); err != nil {
		return nil, err
	}
	return &ApplyResult{}, nil
}

func (f *fsm) applyConsumeArchiveTx(
	b *kvBucket, cmd *ConsumeArchiveCommand,
) (*ApplyResult, error) {
	if err := b.Delete(archiveRecordKey(cmd.StreamID)); err != nil {
		return nil, err
	}
	return &ApplyResult{}, nil
}

func writeEventsTx(
	b *kvBucket, evtPrefix []byte, events []*timebox.Event,
) error {
	for _, ev := range events {
		event, err := timebox.BinEvent.Encode(ev)
		if err != nil {
			return err
		}
		key := aggregateEventKeyFromPrefix(evtPrefix, ev.Sequence)
		if err := b.Put(key, event); err != nil {
			return err
		}
	}
	return nil
}

func applyMutationsTx(
	b *kvBucket, meta *AggregateMeta, encodedID string,
	req *timebox.AppendRequest,
) error {
	if req.Status != nil {
		status := *req.Status
		statusAt := req.StatusAt.UnixMilli()
		if meta.Status != "" && meta.Status != status {
			if err := b.Delete(
				statusIndexKey(meta.Status, encodedID),
			); err != nil {
				return err
			}
		}
		if status == "" {
			meta.Status = ""
			meta.StatusAt = 0
		} else {
			meta.Status = status
			meta.StatusAt = statusAt
			if err := b.Put(
				statusIndexKey(status, encodedID),
				bin.AppendInt64(nil, statusAt),
			); err != nil {
				return err
			}
		}
	}
	return applyLabelMutations(b, meta, encodedID, req.Labels)
}

func applyLabelMutations(
	b *kvBucket, meta *AggregateMeta, encodedID string,
	labels map[string]string,
) error {
	for label, value := range labels {
		oldValue := meta.Labels[label]
		if oldValue == value {
			continue
		}

		if oldValue != "" {
			if err := b.Delete(
				labelIndexKey(label, oldValue, encodedID),
			); err != nil {
				return err
			}
			if err := updateLabelValueCount(
				b, label, oldValue, -1,
			); err != nil {
				return err
			}
		}

		if value == "" {
			delete(meta.Labels, label)
			continue
		}

		if err := b.Put(
			labelIndexKey(label, value, encodedID), []byte{1},
		); err != nil {
			return err
		}
		if err := updateLabelValueCount(b, label, value, 1); err != nil {
			return err
		}
		meta.Labels[label] = value
	}
	return nil
}

func updateLabelValueCount(
	b *kvBucket, label, value string, delta int64,
) error {
	key := labelValueKey(label, value)
	current, err := decodeOptionalInt64(b.Get(key))
	if err != nil {
		return err
	}

	next := current + delta
	if next <= 0 {
		return b.Delete(key)
	}
	return b.Put(key, bin.AppendInt64(nil, next))
}

func loadOrCreateMetaTx(b *kvBucket, encodedID string) (*AggregateMeta, error) {
	meta, ok, err := loadMetaTx(b, encodedID)
	if err != nil {
		return nil, err
	}
	if ok {
		return meta, nil
	}
	return &AggregateMeta{Labels: map[string]string{}}, nil
}

func loadMetaTx(b *kvBucket, encodedID string) (*AggregateMeta, bool, error) {
	value := b.Get(AggregateMetaKey(encodedID))
	if len(value) == 0 {
		return nil, false, nil
	}

	meta, err := unmarshalMeta(value)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func loadLastApplied(db *kvDB) (uint64, error) {
	var applied uint64
	err := db.View(func(tx *kvTx) error {
		var err error
		applied, err = loadLastAppliedTx(tx.Bucket(bucketName))
		return err
	})
	return applied, err
}

func loadLastAppliedTx(b *kvBucket) (uint64, error) {
	applied, err := decodeOptionalInt64(b.Get(lastAppliedKey()))
	if err != nil {
		return 0, err
	}
	return uint64(applied), nil
}

func markApplied(b *kvBucket, logIndex uint64) error {
	return b.Put(lastAppliedKey(), bin.AppendInt64(nil, int64(logIndex)))
}

func conflictResultTx(
	b *kvBucket, encodedID string, meta *AggregateMeta, expectedSeq int64,
) (*ApplyResult, error) {
	conflict := &timebox.VersionConflictError{
		ExpectedSequence: expectedSeq,
		ActualSequence:   meta.CurrentSequence,
	}
	if expectedSeq < meta.CurrentSequence {
		startSeq := max(expectedSeq, meta.BaseSequence)
		evs, err := loadEventsTx(b, encodedID, startSeq)
		if err != nil {
			return nil, err
		}
		conflict.NewEvents = evs
	}
	return &ApplyResult{Error: conflict}, nil
}

func loadEventsTx(
	b *kvBucket, encodedID string, fromSeq int64,
) ([]*timebox.Event, error) {
	if fromSeq < 0 {
		fromSeq = 0
	}
	c := b.Cursor()
	defer func() { _ = c.Close() }()
	pfx := aggregateEventPrefix(encodedID)
	var events []*timebox.Event
	for k, v := c.Seek(aggregateEventKeyFromPrefix(pfx, fromSeq)); k != nil; {
		if !bytes.HasPrefix(k, pfx) {
			break
		}
		ev, err := timebox.BinEvent.Decode(slices.Clone(v))
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
		k, v = c.Next()
	}
	return events, nil
}
