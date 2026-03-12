package timebox

import (
	"encoding/json"
	"sync"
	"time"
)

type (
	// Event represents a single immutable event in the log, including its
	// sequence, timestamp, and serialized payload
	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		Sequence    int64           `json:"sequence"`
		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data"`

		mu    sync.RWMutex
		value any
	}

	// EventType is the string identifier associated with an Event
	EventType string
)

// GetEventValue unmarshals the event data into the requested type. It reuses a
// cached value when the requested type matches, and otherwise unmarshals
// without replacing the cached type. This is safe for concurrent access
func GetEventValue[T any](e *Event) (T, error) {
	e.mu.RLock()
	if val, ok := e.value.(T); ok {
		e.mu.RUnlock()
		return val, nil
	}
	e.mu.RUnlock()
	return resolveEventValue[T](e)
}

func resolveEventValue[T any](e *Event) (T, error) {
	e.mu.Lock()
	val, ok := e.value.(T)
	e.mu.Unlock()

	if ok {
		return val, nil
	}

	data, err := unmarshalEventValue[T](e.Data)
	if err != nil {
		return data, err
	}

	e.mu.Lock()
	if e.value == nil {
		e.value = data
	}
	e.mu.Unlock()

	return data, nil
}

func unmarshalEventValue[T any](data json.RawMessage) (T, error) {
	var res T
	if err := json.Unmarshal(data, &res); err != nil {
		var zero T
		return zero, err
	}
	return res, nil
}
