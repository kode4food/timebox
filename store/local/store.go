package local

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/kode4food/timebox/message"
	"github.com/kode4food/timebox/store"
	"github.com/kode4food/timebox/store/internal/lock"
	"github.com/kode4food/timebox/store/internal/option"
	"git.mills.io/prologic/bitcask"
)

// Error messages
const (
	ErrCouldNotLock = "path could not be locked: %s"
)

type local struct {
	locks    *lock.Manager
	database *bitcask.Bitcask
	path     string
	sinkers  []store.Sinker
	decoder  message.Decoder
}

// Open will prepare a local store at the provided path
func Open(o ...option.Option) (store.Store, error) {
	res := &local{
		locks:   lock.NewManager(),
		decoder: message.RawDecoder,
	}
	if err := option.Apply(res, append(o, open)...); err != nil {
		return nil, err
	}
	return res, nil
}

func open(t option.Target) error {
	res := t.(*local)
	db, err := bitcask.Open(res.path,
		bitcask.WithMaxKeySize(1<<8),
		bitcask.WithMaxValueSize(1<<16),
		bitcask.WithMaxDatafileSize(1<<32),
	)
	if err != nil {
		return err
	}
	res.database = db
	if ok, err := db.TryLock(); !ok || err != nil {
		if err != nil {
			return err
		}
		return fmt.Errorf(ErrCouldNotLock, res.path)
	}
	return nil
}

func (l *local) Close() error {
	return l.database.Close()
}

func (l *local) New() (store.Result, error) {
	id := store.NewID()
	return l.makeInitialResult(id), nil
}

func (l *local) SinkTo(s store.Sinker) {
	l.sinkers = append(l.sinkers, s)
}

func (l *local) sinkEvents(id store.ID, events message.List) {
	for _, s := range l.sinkers {
		s(id, events)
	}
}

func (l *local) None(id store.ID) (store.Result, error) {
	return l.makeInitialResult(id), nil
}

func (l *local) All(id store.ID) (store.Result, error) {
	init := l.makeInitialResult(id)
	return init.withReadLock(func() (store.Result, error) {
		next, err := init.store.getNextVersion(init.id)
		if err != nil {
			return nil, err
		}
		return init.makeResultUntil(next), nil
	})
}

func (l *local) Before(id store.ID, v store.Version) (store.Result, error) {
	init := l.makeInitialResult(id)
	return init.withReadLock(func() (store.Result, error) {
		next, err := init.store.getNextVersion(init.id)
		if err != nil {
			return nil, err
		}
		if v > next {
			return nil, store.NewVersionError(id, next, v)
		}
		return init.makeResultUntil(v), nil
	})
}

func (l *local) makeInitialResult(id store.ID) *result {
	return &result{
		store: l,
		id:    id,
		first: store.InitialVersion,
		next:  store.InitialVersion,
	}
}

func (l *local) getNextVersion(id store.ID) (store.Version, error) {
	key := getVersionKey(id)
	buf, err := l.database.Get(key)
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			return store.InitialVersion, nil
		}
		return store.InitialVersion, err
	}
	var ver store.Version
	err = binary.Read(bytes.NewReader(buf), binary.BigEndian, &ver)
	if err != nil {
		return store.InitialVersion, err
	}
	return ver, nil
}

func getStreamPrefix(id store.ID) string {
	return id.String() + "/"
}

func getVersionKey(id store.ID) []byte {
	return []byte(getStreamPrefix(id) + "Version")
}

func getStreamKey(id store.ID, ver store.Version) []byte {
	return []byte(getStreamPrefix(id) + fmt.Sprintf("%020d", ver))
}
