// Package scheduler delivers durable Timebox messages when they become due
package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/kode4food/timebox"
)

type (
	// Scheduler maintains a disposable heap over durable schedule aggregates
	Scheduler struct {
		store      *timebox.Store
		process    Processor
		clock      Clock
		newTimer   TimerConstructor
		rescan     time.Duration
		retryDelay time.Duration
		wake       chan struct{}
		heap       *scheduleHeap
	}

	// Config configures a disposable schedule runner
	Config struct {
		Store            *timebox.Store
		Processor        Processor
		Clock            Clock
		TimerConstructor TimerConstructor
		RescanInterval   time.Duration
		RetryDelay       time.Duration
	}

	// Processor changes state in response to one due message
	Processor func(*timebox.Transaction, *timebox.Message) error
)

const (
	// DefaultRescanInterval controls discovery of changes from other processes
	DefaultRescanInterval = time.Second

	// DefaultRetryDelay controls local retry after scheduler errors
	DefaultRetryDelay = time.Second
)

var (
	// ErrStoreRequired indicates a Scheduler has no Store
	ErrStoreRequired = errors.New("scheduler store is required")

	// ErrProcessorRequired indicates a Scheduler has no Processor
	ErrProcessorRequired = errors.New("scheduler processor is required")

	// ErrRetry leaves a due message active without logging the attempt
	ErrRetry = errors.New("retry scheduled message")

	// ErrInvalidRescanInterval indicates a non-positive rescan interval
	ErrInvalidRescanInterval = errors.New(
		"scheduler rescan interval must be positive",
	)

	// ErrInvalidRetryDelay indicates a non-positive retry delay
	ErrInvalidRetryDelay = errors.New("scheduler retry delay must be positive")
)

// New constructs a disposable schedule runner
func New(cfg Config) (*Scheduler, error) {
	if cfg.Store == nil {
		return nil, ErrStoreRequired
	}
	if cfg.Processor == nil {
		return nil, ErrProcessorRequired
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.TimerConstructor == nil {
		cfg.TimerConstructor = newTimer
	}
	if cfg.RescanInterval == 0 {
		cfg.RescanInterval = DefaultRescanInterval
	}
	if cfg.RescanInterval < 0 {
		return nil, ErrInvalidRescanInterval
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = DefaultRetryDelay
	}
	if cfg.RetryDelay < 0 {
		return nil, ErrInvalidRetryDelay
	}
	return &Scheduler{
		store:      cfg.Store,
		process:    cfg.Processor,
		clock:      cfg.Clock,
		newTimer:   cfg.TimerConstructor,
		rescan:     cfg.RescanInterval,
		retryDelay: cfg.RetryDelay,
		wake:       make(chan struct{}, 1),
		heap:       newScheduleHeap(),
	}, nil
}

// Wake requests prompt reconciliation with the durable schedule index
func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run delivers due messages until ctx ends
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.store.WaitReady(ctx); err != nil {
		return err
	}
	timer := s.newTimer(0)
	defer timer.Stop()
	if err := s.reconcileSchedules(); err != nil {
		s.logReconcileError(ctx, err)
		resetTimer(timer, s.retryDelay)
	} else {
		resetTimer(timer, s.calculateDelay())
	}

	for {
		process := false
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
		case <-s.store.ScheduleChanges():
		case <-timer.Channel():
			process = true
		}
		if err := s.reconcileSchedules(); err != nil {
			s.logReconcileError(ctx, err)
			resetTimer(timer, s.retryDelay)
			continue
		}
		if process {
			s.processDue(ctx)
		}
		resetTimer(timer, s.calculateDelay())
	}
}

func (s *Scheduler) reconcileSchedules() error {
	schedules, err := s.store.ListSchedules(s.clock().Add(s.rescan))
	if err != nil {
		return err
	}

	seen := make(map[timebox.ScheduleKey]struct{}, len(schedules))
	for _, schedule := range schedules {
		seen[schedule.Key] = struct{}{}
		item, ok := s.heap.byKey[schedule.Key]
		if ok && item.schedule.Version == schedule.Version {
			continue
		}
		s.heap.Replace(schedule)
	}
	for key := range s.heap.byKey {
		if _, ok := seen[key]; !ok {
			s.heap.Remove(key)
		}
	}
	return nil
}

func (s *Scheduler) processDue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			item := s.heap.Peek()
			if item == nil || item.readyAt.After(s.clock()) {
				return
			}
			item = heap.Pop(s.heap).(*heapItem)

			err := s.store.Transact(func(tx *timebox.Transaction) error {
				if err := tx.ConsumeSchedule(
					item.schedule.Key, item.schedule.Version,
				); err != nil {
					return err
				}
				return s.process(tx, item.schedule.Message)
			})
			if err != nil {
				s.logError(ctx, item.schedule.Key, err)
			}
			s.refreshProcessed(ctx, item)
		}
	}
}

func (s *Scheduler) refreshProcessed(ctx context.Context, item *heapItem) {
	schedule, err := s.store.LoadSchedule(item.schedule.Key)
	if !errors.Is(err, nil) {
		s.logError(ctx, item.schedule.Key, err)
		s.retryItem(item)
		return
	}
	if schedule == nil {
		return
	}
	if schedule.Version != item.schedule.Version {
		s.heap.Replace(schedule)
		return
	}
	s.retryItem(item)
}

func (s *Scheduler) retryItem(item *heapItem) {
	item.readyAt = s.clock().Add(s.retryDelay)
	heap.Push(s.heap, item)
}

func (s *Scheduler) logError(
	ctx context.Context, key timebox.ScheduleKey, err error,
) {
	if _, ok := errors.AsType[*timebox.ScheduleVersionConflictError](err); ok {
		return
	}
	if errors.Is(err, ErrRetry) {
		return
	}
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return
	}
	slog.ErrorContext(ctx, "Schedule processing failed",
		slog.String("schedule_key", string(key)),
		slog.Any("error", err),
	)
}

func (*Scheduler) logReconcileError(ctx context.Context, err error) {
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return
	}
	slog.ErrorContext(ctx, "Schedule reconciliation failed",
		slog.Any("error", err),
	)
}

func (s *Scheduler) calculateDelay() time.Duration {
	item := s.heap.Peek()
	if item == nil {
		return s.rescan
	}
	return min(max(item.readyAt.Sub(s.clock()), 0), s.rescan)
}

func resetTimer(timer Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.Channel():
		default:
		}
	}
	timer.Reset(delay)
}
