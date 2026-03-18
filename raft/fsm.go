package raft

import (
	"bytes"
	"encoding/json"
	"slices"

	"go.etcd.io/bbolt"

	"github.com/kode4food/timebox"
)

type (
	fsm struct {
		db         *bbolt.DB
		trimEvents bool
	}

	// decodedEntry pairs a raft log index with its command bytes
	decodedEntry struct {
		index uint64
		cmd   Command
	}
)

func newFSM(db *bbolt.DB, trimEvents bool) *fsm {
	return &fsm{
		db:         db,
		trimEvents: trimEvents,
	}
}

func (f *fsm) applyEntries(ents []decodedEntry) ([]*ApplyResult, error) {
	if len(ents) == 0 {
		return nil, nil
	}

	results := make([]*ApplyResult, len(ents))

	err := f.db.Update(func(tx *bbolt.Tx) error {
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
	b *bbolt.Bucket, req *timebox.AppendRequest,
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
		b, aggregateEventPrefix(encodedID), currentSeq, req.Events,
	); err != nil {
		return nil, err
	}
	if err := applyMutationsTx(b, meta, encodedID, *req); err != nil {
		return nil, err
	}

	meta.CurrentSequence = currentSeq + int64(len(req.Events))
	if err := b.Put(AggregateMetaKey(encodedID), marshalMeta(meta)); err != nil {
		return nil, err
	}

	return &ApplyResult{}, nil
}

func (f *fsm) applySnapshotTx(
	b *bbolt.Bucket, cmd *SnapshotCommand,
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

	if f.trimEvents {
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

func writeEventsTx(
	b *bbolt.Bucket, evtPrefix []byte, baseSeq int64, events []string,
) error {
	for i, event := range events {
		key := aggregateEventKeyFromPrefix(evtPrefix, baseSeq+int64(i))
		if err := b.Put(key, []byte(event)); err != nil {
			return err
		}
	}
	return nil
}

func applyMutationsTx(
	b *bbolt.Bucket, meta *AggregateMeta, encodedID string,
	req timebox.AppendRequest,
) error {
	if req.Status != nil {
		status := *req.Status
		statusAt := parseStatusAt(req.StatusAt)
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
				encodeInt64(statusAt),
			); err != nil {
				return err
			}
		}
	}
	return applyLabelMutations(b, meta, encodedID, req.Labels)
}

func applyLabelMutations(
	b *bbolt.Bucket, meta *AggregateMeta, encodedID string,
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
	b *bbolt.Bucket, label, value string, delta int64,
) error {
	key := labelValueKey(label, value)
	current, err := decodeInt64(b.Get(key))
	if err != nil {
		return err
	}

	next := current + delta
	if next <= 0 {
		return b.Delete(key)
	}
	return b.Put(key, encodeInt64(next))
}

func loadOrCreateMetaTx(
	b *bbolt.Bucket, encodedID string,
) (*AggregateMeta, error) {
	meta, ok, err := loadMetaTx(b, encodedID)
	if err != nil {
		return nil, err
	}
	if ok {
		return meta, nil
	}
	return &AggregateMeta{Labels: map[string]string{}}, nil
}

func loadMetaTx(
	b *bbolt.Bucket, encodedID string,
) (*AggregateMeta, bool, error) {
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

func loadLastApplied(db *bbolt.DB) (uint64, error) {
	var applied uint64
	err := db.View(func(tx *bbolt.Tx) error {
		var err error
		applied, err = loadLastAppliedTx(tx.Bucket(bucketName))
		return err
	})
	return applied, err
}

func loadLastAppliedTx(b *bbolt.Bucket) (uint64, error) {
	decoded, err := decodeInt64(b.Get(lastAppliedKey()))
	if err != nil {
		return 0, err
	}
	return uint64(decoded), nil
}

func markApplied(b *bbolt.Bucket, logIndex uint64) error {
	return b.Put(lastAppliedKey(), encodeInt64(int64(logIndex)))
}

func conflictResultTx(
	b *bbolt.Bucket, encodedID string, meta *AggregateMeta, expectedSeq int64,
) (*ApplyResult, error) {
	conflict := &timebox.AppendResult{
		ActualSequence: meta.CurrentSequence,
	}
	if expectedSeq < meta.CurrentSequence {
		startSeq := max(expectedSeq, meta.BaseSequence)
		evs, err := loadRawEventsTx(b, encodedID, startSeq)
		if err != nil {
			return nil, err
		}
		conflict.NewEvents = evs
	}
	return &ApplyResult{Conflict: conflict}, nil
}

func loadRawEventsTx(
	b *bbolt.Bucket, encodedID string, fromSeq int64,
) ([]json.RawMessage, error) {
	if fromSeq < 0 {
		fromSeq = 0
	}

	c := b.Cursor()
	pfx := aggregateEventPrefix(encodedID)
	var events []json.RawMessage
	for k, v := c.Seek(aggregateEventKeyFromPrefix(pfx, fromSeq)); k != nil; {
		if !bytes.HasPrefix(k, pfx) {
			break
		}
		events = append(events, slices.Clone(v))
		k, v = c.Next()
	}
	return events, nil
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	return slices.Clone(data)
}
