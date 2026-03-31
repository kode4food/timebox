package raft

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

type (
	kvDB struct {
		db *badger.DB
	}

	kvTx struct {
		txn *badger.Txn
	}

	kvBucket struct {
		txn *badger.Txn
		pfx []byte
	}

	kvCursor struct {
		it  *badger.Iterator
		pfx []byte
	}
)

const (
	writeCh            = 16
	memTableSize       = 128 << 20
	baseTableSize      = 8 << 20
	baseLevelSize      = 32 << 20
	numMemTables       = 8
	numLevelZeroTables = 10
	l0StallTables      = 20
	blockCacheSize     = 128 << 20
)

func openKVDB(path string) (*kvDB, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(path).
		WithValueDir(path).
		WithLogger(nil).
		WithMetricsEnabled(false).
		WithCompression(options.None).
		WithDetectConflicts(false).
		WithMemTableSize(memTableSize).
		WithBaseTableSize(baseTableSize).
		WithBaseLevelSize(baseLevelSize).
		WithNumMemtables(numMemTables).
		WithNumLevelZeroTables(numLevelZeroTables).
		WithNumLevelZeroTablesStall(l0StallTables).
		WithNumCompactors(defaultCompactors()).
		WithBlockCacheSize(blockCacheSize)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &kvDB{db: db}, nil
}

func (d *kvDB) Close() error {
	return d.db.Close()
}

func (d *kvDB) View(fn func(*kvTx) error) error {
	return d.db.View(func(txn *badger.Txn) error {
		return fn(&kvTx{txn: txn})
	})
}

func (d *kvDB) Update(fn func(*kvTx) error) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return fn(&kvTx{txn: txn})
	})
}

func (tx *kvTx) Bucket(name []byte) *kvBucket {
	pfx := make([]byte, len(name)+1)
	copy(pfx, name)
	pfx[len(name)] = '/'
	return &kvBucket{
		txn: tx.txn,
		pfx: pfx,
	}
}

func (b *kvBucket) Get(key []byte) []byte {
	item, err := b.txn.Get(b.key(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return nil
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil
	}
	return val
}

func (b *kvBucket) Put(key, value []byte) error {
	return b.txn.Set(b.key(key), value)
}

func (b *kvBucket) Delete(key []byte) error {
	return b.txn.Delete(b.key(key))
}

func (b *kvBucket) Cursor() *kvCursor {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	return &kvCursor{
		it:  b.txn.NewIterator(opts),
		pfx: append([]byte(nil), b.pfx...),
	}
}

func (c *kvCursor) Close() error {
	c.it.Close()
	return nil
}

func (c *kvCursor) First() ([]byte, []byte) {
	c.it.Seek(c.pfx)
	return c.item()
}

func (c *kvCursor) Seek(key []byte) ([]byte, []byte) {
	seek := make([]byte, 0, len(c.pfx)+len(key))
	seek = append(seek, c.pfx...)
	seek = append(seek, key...)
	c.it.Seek(seek)
	return c.item()
}

func (c *kvCursor) Next() ([]byte, []byte) {
	if c.it.Valid() {
		c.it.Next()
	}
	return c.item()
}

func (b *kvBucket) key(key []byte) []byte {
	full := make([]byte, 0, len(b.pfx)+len(key))
	full = append(full, b.pfx...)
	full = append(full, key...)
	return full
}

func (c *kvCursor) item() ([]byte, []byte) {
	if !c.it.Valid() {
		return nil, nil
	}
	it := c.it.Item()
	key := it.KeyCopy(nil)
	if !bytes.HasPrefix(key, c.pfx) {
		return nil, nil
	}
	val, err := it.ValueCopy(nil)
	if err != nil {
		return nil, nil
	}
	return append([]byte(nil), key[len(c.pfx):]...), val
}

func replaceKVDBFrom(path string, r io.Reader) (*kvDB, error) {
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}
	db, err := openKVDB(path)
	if err != nil {
		return nil, err
	}
	if err := db.db.Load(r, writeCh); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func backupKVDBTo(db *kvDB, w io.Writer) error {
	_, err := db.db.Backup(w, 0)
	return err
}

func removeKVPath(path string) error {
	return os.RemoveAll(path)
}

func defaultCompactors() int {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 4 {
		return 4
	}
	if n > 8 {
		return 8
	}
	return n
}

func kvPath(dataDir, dirName, dbName string) string {
	return filepath.Join(dataDir, dirName, dbName)
}
