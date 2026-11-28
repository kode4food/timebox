// Package dbos provides an experimental Timebox backend using DBOS workflows and queues.
// Journal and archive workflow inputs are durable data and must be retained for replay.
package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kode4food/timebox"
)

type (
	// Backend persists Timebox state as DBOS workflow records and queues
	Backend struct {
		timebox.AlwaysReady

		pool      *pgxpool.Pool
		logs      map[timebox.AggregateID]*aggregateJournal
		stateMu   sync.Mutex
		runtime   dbos.Context
		cfg       Config
		done      chan struct{}
		closed    sync.Once
		closeErr  error
		deliverMu sync.Mutex
		deliverWG sync.WaitGroup
	}

	// Config selects a PostgreSQL database and an independent namespace for the DBOS backend
	Config struct {
		URL       string
		Namespace string
		MaxConns  int32
		Publisher timebox.Publisher
		Logger    *slog.Logger
	}
)

const (
	defaultURL         = "postgres://localhost:5432/postgres?sslmode=disable"
	defaultNamespace   = "timebox_dbos"
	defaultMaxConns    = 32
	startupTimeout     = 15 * time.Second
	shutdownTimeout    = 10 * time.Second
	applicationVersion = "timebox-v1"
)

var (
	// ErrClosed indicates the DBOS backend has closed
	ErrClosed = errors.New("dbos backend is closed")
	// ErrInvalidNamespace indicates the namespace is unsuitable for PostgreSQL and DBOS
	ErrInvalidNamespace = errors.New("namespace must match [a-z][a-z0-9_]{2,47}")
	// ErrInvalidMaxConns indicates the DBOS runtime has too few database connections
	ErrInvalidMaxConns = errors.New("dbos backend requires at least four connections")

	namespacePattern = regexp.MustCompile("^[a-z][a-z0-9_]{2,47}$")

	_ timebox.Backend = (*Backend)(nil)
)

// Open starts a DBOS-backed Timebox backend
func Open(cfg Config) (*Backend, error) {
	if cfg.URL == "" {
		cfg.URL = defaultURL
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultNamespace
	}
	if !namespacePattern.MatchString(cfg.Namespace) {
		return nil, ErrInvalidNamespace
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = defaultMaxConns
	}
	if cfg.MaxConns < 4 {
		return nil, ErrInvalidMaxConns
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.MaxConns
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		logs: map[timebox.AggregateID]*aggregateJournal{},
		pool: pool,
		cfg:  cfg,
		done: make(chan struct{}),
	}
	if err := b.start(); err != nil {
		return nil, errors.Join(err, b.Close())
	}
	return b, nil
}

// NewStore creates a Timebox Store backed by DBOS
func (b *Backend) NewStore(cfgs ...timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(b, cfgs...)
}

// Close stops delivery and write workflows, then releases database connections
func (b *Backend) Close() error {
	b.closed.Do(func() {
		b.deliverMu.Lock()
		close(b.done)
		b.deliverMu.Unlock()
		b.deliverWG.Wait()
		if b.runtime != nil {
			b.closeErr = dbos.Shutdown(b.runtime, shutdownTimeout)
		}
		b.pool.Close()
	})
	return b.closeErr
}

func (b *Backend) start() error {
	runtime, err := b.newRuntime("-write")
	if err != nil {
		return err
	}
	b.runtime = runtime
	if _, err := dbos.RegisterQueue(runtime, journalQueueName,
		dbos.WithQueueBasePollingInterval(deliveryPollInterval)); err != nil {
		return err
	}
	if _, err := dbos.RegisterQueue(runtime, archiveQueueName,
		dbos.WithGlobalConcurrency(1),
		dbos.WithQueueBasePollingInterval(archivePollInterval)); err != nil {
		return err
	}
	// Journal records are stored state, never executed, so no runtime listens on their queue.
	dbos.ListenQueues(runtime, archiveQueueName)
	dbos.RegisterWorkflow(runtime, journalWorkflow, dbos.WithWorkflowName(journalWorkflowName))
	dbos.RegisterWorkflow(runtime, archiveWorkflow, dbos.WithWorkflowName(archiveWorkflowName))
	if err := dbos.Launch(runtime); err != nil {
		return fmt.Errorf("launch dbos writes: %w", err)
	}
	return b.loadLegacyJournal()
}

func (b *Backend) newRuntime(appSuffix string) (dbos.Context, error) {
	return dbos.NewContext(context.Background(), dbos.Config{
		AppName:                b.cfg.Namespace + appSuffix,
		ApplicationVersion:     applicationVersion,
		SystemDBPool:           b.pool,
		DatabaseSchema:         b.cfg.Namespace + "_sdk",
		SystemDBStartupTimeout: startupTimeout,
		Logger:                 b.cfg.Logger,
	})
}

func (b *Backend) checkOpen() error {
	select {
	case <-b.done:
		return ErrClosed
	default:
		return nil
	}
}
