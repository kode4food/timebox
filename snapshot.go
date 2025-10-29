package timebox

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type (
	SnapshotWorker struct {
		store  Store
		ctx    context.Context
		queue  chan snapshotRequest
		cancel context.CancelFunc
		config StoreConfig
		wg     sync.WaitGroup
	}

	snapshotRequest struct {
		value    any
		sequence int64
		id       AggregateID
	}
)

func NewSnapshotWorker(
	store Store, config StoreConfig,
) *SnapshotWorker {
	ctx, cancel := context.WithCancel(context.Background())

	sw := &SnapshotWorker{
		store:  store,
		config: config,
		queue:  make(chan snapshotRequest, config.MaxQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}

	for i := 0; i < config.WorkerCount; i++ {
		sw.wg.Add(1)
		go sw.worker(i)
	}

	return sw
}

func (sw *SnapshotWorker) worker(id int) {
	defer sw.wg.Done()

	for {
		select {
		case <-sw.ctx.Done():
			return
		case req := <-sw.queue:
			sw.saveSnapshot(id, req)
		}
	}
}

func (sw *SnapshotWorker) saveSnapshot(workerID int, req snapshotRequest) {
	ctx, cancel := context.WithTimeout(sw.ctx, sw.config.SaveTimeout)
	defer cancel()

	start := time.Now()
	err := sw.store.PutSnapshot(ctx, req.id, req.value, req.sequence)
	duration := time.Since(start)

	if err != nil {
		slog.Error("Failed to save snapshot",
			slog.Int("worker_id", workerID),
			slog.Any("aggregate_id", req.id),
			slog.Int64("sequence", req.sequence),
			slog.Duration("duration", duration),
			slog.Any("error", err),
		)
		return
	}

	slog.Debug("Snapshot saved",
		slog.Int("worker_id", workerID),
		slog.Any("aggregate_id", req.id),
		slog.Int64("sequence", req.sequence),
		slog.Duration("duration", duration),
	)
}

func (sw *SnapshotWorker) enqueue(
	id AggregateID, value any, sequence int64,
) bool {
	req := snapshotRequest{
		id:       id,
		value:    value,
		sequence: sequence,
	}

	select {
	case sw.queue <- req:
		return true
	default:
		slog.Warn("Snapshot queue full, dropping request",
			slog.Any("aggregate_id", id),
			slog.Int64("sequence", sequence),
			slog.Int("queue_size", len(sw.queue)),
		)
		return false
	}
}

func (sw *SnapshotWorker) Stop() {
	sw.cancel()
	sw.wg.Wait()
	close(sw.queue)
}
