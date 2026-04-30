package raft

import (
	"bytes"
	"context"

	"github.com/kode4food/timebox"
	bin "github.com/kode4food/timebox/internal/binary"

	"go.etcd.io/raft/v3"
)

// Archive archives an aggregate and removes it from active storage
func (p *Persistence) Archive(id timebox.AggregateID) error {
	propID := p.newProposalID()
	_, err := p.applyWithTimeout(
		context.Background(),
		MakeArchiveCommand(propID, &ArchiveCommand{ID: id}),
		propID,
		nil,
	)
	return err
}

// ConsumeArchive blocks until one archive record is available or ctx is done
func (p *Persistence) ConsumeArchive(
	ctx context.Context, h timebox.ArchiveHandler,
) error {
	if h == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	for {
		rec, err := p.nextArchive()
		if err != nil {
			return err
		}
		if rec != nil {
			if err := h(ctx, rec); err != nil {
				return err
			}
			return p.consumeArchive(ctx, rec.StreamID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			if err, ok := p.stopErr.Load().(error); ok && err != nil {
				return err
			}
			return raft.ErrStopped
		case <-p.archiveCh:
		}
	}
}

func (p *Persistence) nextArchive() (*timebox.ArchiveRecord, error) {
	var rec *timebox.ArchiveRecord
	err := p.db.View(func(tx *kvTx) error {
		var err error
		rec, err = nextArchiveTx(tx.Bucket(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (p *Persistence) consumeArchive(
	ctx context.Context, streamID string,
) error {
	propID := p.newProposalID()
	_, err := p.applyWithTimeout(
		ctx,
		MakeConsumeArchiveCommand(propID, &ConsumeArchiveCommand{
			StreamID: streamID,
		}),
		propID,
		nil,
	)
	return err
}

func (p *Persistence) notifyArchive() {
	select {
	case p.archiveCh <- struct{}{}:
	default:
	}
}

func nextArchiveTx(b *kvBucket) (*timebox.ArchiveRecord, error) {
	c := b.Cursor()
	defer func() { _ = c.Close() }()

	pfx := archivePrefix()
	k, v := c.Seek(pfx)
	if k == nil || !bytes.HasPrefix(k, pfx) {
		return nil, nil
	}
	streamID := string(bytes.TrimPrefix(k, pfx))
	return decodeArchiveRecord(streamID, v)
}

func putArchiveRecordTx(b *kvBucket, rec *timebox.ArchiveRecord) error {
	data, err := encodeArchiveRecord(rec)
	if err != nil {
		return err
	}
	return b.Put(archiveRecordKey(rec.StreamID), data)
}

func deleteAggregateTx(
	b *kvBucket, encodedID string, meta *AggregateMeta,
) error {
	if meta.Status != "" {
		if err := b.Delete(statusIndexKey(meta.Status, encodedID)); err != nil {
			return err
		}
	}
	for label, value := range meta.Labels {
		if err := b.Delete(labelIndexKey(label, value, encodedID)); err != nil {
			return err
		}
		if err := updateLabelValueCount(b, label, value, -1); err != nil {
			return err
		}
	}

	pfx := aggregateEventPrefix(encodedID)
	keys, err := collectKeysTx(b, pfx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	if err := b.Delete(aggregateSnapshotKey(encodedID)); err != nil {
		return err
	}
	return b.Delete(AggregateMetaKey(encodedID))
}

func collectKeysTx(b *kvBucket, pfx []byte) ([][]byte, error) {
	c := b.Cursor()
	defer func() { _ = c.Close() }()

	var keys [][]byte
	for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
		keys = append(keys, append([]byte(nil), k...))
		k, _ = c.Next()
	}
	return keys, nil
}

func encodeArchiveRecord(rec *timebox.ArchiveRecord) ([]byte, error) {
	buf := make([]byte, 0, 128)
	buf = appendAggregateID(buf, rec.AggregateID)
	buf = bin.AppendBytes(buf, rec.SnapshotData)
	buf = bin.AppendInt64(buf, rec.SnapshotSequence)
	return timebox.BinEvent.AppendAll(buf, rec.Events)
}

func decodeArchiveRecord(
	streamID string, data []byte,
) (*timebox.ArchiveRecord, error) {
	id, data, err := readAggregateID(data)
	if err != nil {
		return nil, err
	}
	snap, data, err := bin.ReadBytes(data)
	if err != nil {
		return nil, err
	}
	seq, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	events, _, err := timebox.BinEvent.ReadAll(data)
	if err != nil {
		return nil, err
	}
	return &timebox.ArchiveRecord{
		StreamID:         streamID,
		AggregateID:      id,
		SnapshotData:     snap,
		SnapshotSequence: seq,
		Events:           events,
	}, nil
}

func archiveStreamID(index uint64) string {
	var b [seqWidth]byte
	writeSequence(b[:], int64(index))
	return string(b[:])
}
