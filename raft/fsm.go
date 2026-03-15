package raft

import (
	"bytes"
	"encoding/json"
	"slices"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

type (
	fsm struct {
		db         *bbolt.DB
		trimEvents bool
		setStatus  func(timebox.AggregateID, string)
	}

	statusUpdate struct {
		id     timebox.AggregateID
		status string
	}
)

func (f *fsm) ApplyEntry(ent raftpb.Entry) *applyResult {
	res, err := f.ApplyEntries([]raftpb.Entry{ent})
	if err != nil {
		return encodeApplyError(applyCodeInternal, err)
	}
	return normalizeApplyResult(res[0])
}

func (f *fsm) ApplyEntries(ents []raftpb.Entry) ([]*applyResult, error) {
	if len(ents) == 0 {
		return nil, nil
	}

	results := make([]*applyResult, len(ents))
	updates := make([]statusUpdate, 0, len(ents))

	err := f.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		lastApplied, err := loadLastAppliedTx(b)
		if err != nil {
			return err
		}

		for i, ent := range ents {
			if ent.Index <= lastApplied {
				results[i] = &applyResult{}
				continue
			}
			if len(ent.Data) == 0 {
				results[i] = &applyResult{}
				lastApplied = ent.Index
				continue
			}

			cmd, err := decodeCommand(ent.Data)
			if err != nil {
				results[i] = encodeApplyError(applyCodeInvalidCommand, err)
				break
			}

			var res *applyResult
			var update *statusUpdate

			switch cmd.Type {
			case commandAppend:
				if cmd.Append == nil {
					results[i] = encodeApplyError(
						applyCodeInvalidCommand, ErrAppendCommandMissing,
					)
					break
				}
				res, update, err = f.applyAppendTx(b, cmd.Append)
			case commandSnapshot:
				if cmd.Snapshot == nil {
					results[i] = encodeApplyError(
						applyCodeInvalidCommand, ErrSnapshotCommandMissing,
					)
					break
				}
				res, err = f.applySnapshotTx(b, cmd.Snapshot)
			default:
				results[i] = encodeApplyError(
					applyCodeInvalidCommand, ErrCommandTypeUnknown,
				)
			}

			if err != nil {
				return err
			}

			results[i] = res
			lastApplied = ent.Index
			if update != nil {
				updates = append(updates, *update)
			}
		}

		return markApplied(b, lastApplied)
	})
	if err != nil {
		return nil, err
	}

	for _, update := range updates {
		f.setStatus(update.id, update.status)
	}
	return results, nil
}

func (f *fsm) applyAppendTx(
	b *bbolt.Bucket, cmd *appendCommand,
) (*applyResult, *statusUpdate, error) {
	req := cmd.Request
	encodedID := encodeAggregateID(req.ID)
	metaKey := aggregateMetaKeyEncoded(encodedID)
	eventPrefix := aggregateEventPrefixEncoded(encodedID)
	meta, err := loadOrCreateMetaTxEncoded(b, encodedID)
	if err != nil {
		return nil, nil, err
	}

	currentSeq := meta.CurrentSequence
	if req.ExpectedSequence != currentSeq {
		res, err := conflictResultTx(
			b, req.ID, meta, req.ExpectedSequence,
		)
		return res, nil, err
	}

	for i, event := range req.Events {
		seq := currentSeq + int64(i)
		if err := b.Put(
			aggregateEventKeyFromPrefix(eventPrefix, seq), []byte(event),
		); err != nil {
			return nil, nil, err
		}
	}

	if req.Status != nil {
		if err := applyStatusMutation(
			b, meta, encodedID, *req.Status, parseStatusAt(req.StatusAt),
		); err != nil {
			return nil, nil, err
		}
	}

	if err := applyLabelMutations(
		b, meta, encodedID, req.Labels,
	); err != nil {
		return nil, nil, err
	}

	meta.CurrentSequence = currentSeq + int64(len(req.Events))
	data, err := marshalMeta(meta)
	if err != nil {
		return nil, nil, err
	}
	if err := b.Put(metaKey, data); err != nil {
		return nil, nil, err
	}

	return &applyResult{}, &statusUpdate{
		id:     req.ID,
		status: meta.Status,
	}, nil
}

func (f *fsm) applySnapshotTx(
	b *bbolt.Bucket, cmd *snapshotCommand,
) (*applyResult, error) {
	encodedID := encodeAggregateID(cmd.ID)
	metaKey := aggregateMetaKeyEncoded(encodedID)
	snapshotKey := aggregateSnapshotKeyEncoded(encodedID)
	eventPrefix := aggregateEventPrefixEncoded(encodedID)
	meta, err := loadOrCreateMetaTxEncoded(b, encodedID)
	if err != nil {
		return nil, err
	}
	if cmd.Sequence < meta.SnapshotSequence {
		return &applyResult{}, nil
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
					aggregateEventKeyFromPrefix(eventPrefix, seq),
				); err != nil {
					return nil, err
				}
			}
			meta.BaseSequence = trimTo
		}
	}

	data, err := marshalMeta(meta)
	if err != nil {
		return nil, err
	}
	if err := b.Put(metaKey, data); err != nil {
		return nil, err
	}

	return &applyResult{}, nil
}

func newFSM(
	db *bbolt.DB, trimEvents bool, setStatus func(timebox.AggregateID, string),
) *fsm {
	return &fsm{
		db:         db,
		trimEvents: trimEvents,
		setStatus:  setStatus,
	}
}

func applyStatusMutation(
	b *bbolt.Bucket, meta *aggregateMeta, encodedID string, status string,
	statusAt int64,
) error {
	if meta.Status != "" && meta.Status != status {
		if err := b.Delete(
			statusIndexKeyEncoded(meta.Status, encodedID),
		); err != nil {
			return err
		}
	}

	if status == "" {
		meta.Status = ""
		meta.StatusAt = 0
		return nil
	}

	meta.Status = status
	meta.StatusAt = statusAt
	return b.Put(
		statusIndexKeyEncoded(status, encodedID), encodeInt64(statusAt),
	)
}

func applyLabelMutations(
	b *bbolt.Bucket, meta *aggregateMeta, encodedID string,
	labels map[string]string,
) error {
	for label, value := range labels {
		oldValue := meta.Labels[label]
		if oldValue == value {
			continue
		}

		if oldValue != "" {
			if err := b.Delete(
				labelIndexKeyEncoded(label, oldValue, encodedID),
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
			labelIndexKeyEncoded(label, value, encodedID), []byte{1},
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

func loadOrCreateMetaTxEncoded(
	b *bbolt.Bucket, encodedID string,
) (*aggregateMeta, error) {
	meta, ok, err := loadMetaTxEncoded(b, encodedID)
	if err != nil {
		return nil, err
	}
	if ok {
		return meta, nil
	}
	return &aggregateMeta{Labels: map[string]string{}}, nil
}

func loadMetaTx(
	b *bbolt.Bucket, id timebox.AggregateID,
) (*aggregateMeta, bool, error) {
	return loadMetaTxEncoded(b, encodeAggregateID(id))
}

func loadMetaTxEncoded(
	b *bbolt.Bucket, encodedID string,
) (*aggregateMeta, bool, error) {
	value := b.Get(aggregateMetaKeyEncoded(encodedID))
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
	b *bbolt.Bucket, id timebox.AggregateID, meta *aggregateMeta,
	expectedSeq int64,
) (*applyResult, error) {
	conflict := &timebox.AppendResult{
		ActualSequence: meta.CurrentSequence,
	}
	if expectedSeq < meta.CurrentSequence {
		startSeq := max(expectedSeq, meta.BaseSequence)
		evs, err := loadRawEventsTx(b, id, startSeq)
		if err != nil {
			return nil, err
		}
		conflict.NewEvents = evs
	}
	return &applyResult{Conflict: conflict}, nil
}

func loadRawEventsTx(
	b *bbolt.Bucket, id timebox.AggregateID, fromSeq int64,
) ([]json.RawMessage, error) {
	if fromSeq < 0 {
		fromSeq = 0
	}

	c := b.Cursor()
	pfx := aggregateEventPrefix(id)
	var events []json.RawMessage
	for k, v := c.Seek(aggregateEventKey(id, fromSeq)); k != nil; {
		if !bytes.HasPrefix(k, pfx) {
			break
		}
		events = append(events, json.RawMessage(slices.Clone(v)))
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
