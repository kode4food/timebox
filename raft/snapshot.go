package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"go.etcd.io/bbolt"
)

type snapshot struct {
	tx   *bbolt.Tx
	size int64
}

const raftSnapDBSuffix = ".snap.db"

func openSnapshot(db *bbolt.DB, index uint64) (*snapshot, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket(bucketName)
	applied, err := loadLastAppliedTx(b)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if applied != index {
		_ = tx.Rollback()
		return nil, ErrCorruptState
	}
	return &snapshot{
		tx:   tx,
		size: tx.Size(),
	}, nil
}

func (s *snapshot) Size() int64 {
	return s.size
}

func (s *snapshot) WriteTo(w io.Writer) (int64, error) {
	return s.tx.WriteTo(w)
}

func (s *snapshot) Close() error {
	return s.tx.Rollback()
}

func snapshotPath(dir string, index uint64) string {
	name := fmt.Sprintf("%016x%s", index, raftSnapDBSuffix)
	return filepath.Join(dir, name)
}

func saveSnapshot(dir string, r io.Reader, index uint64) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	path := snapshotPath(dir, index)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return errors.Join(os.Remove(tmp), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.Join(os.Remove(tmp), err)
	}
	if err := f.Close(); err != nil {
		return errors.Join(os.Remove(tmp), err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return errors.Join(os.Remove(tmp), err)
	}
	return nil
}

func copySnapshot(dst, src *bbolt.DB) error {
	return src.View(func(srcTx *bbolt.Tx) error {
		srcB := srcTx.Bucket(bucketName)
		return dst.Update(func(dstTx *bbolt.Tx) error {
			dstB := dstTx.Bucket(bucketName)
			if err := clearBucket(dstB); err != nil {
				return err
			}
			c := srcB.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if err := dstB.Put(
					slices.Clone(k), slices.Clone(v),
				); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func clearBucket(b *bbolt.Bucket) error {
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}
