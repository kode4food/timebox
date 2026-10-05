package timebox

import (
	"encoding/json"
	"sync"
	"time"
)

type (
	// Event is a recorded Message with its sequence and timestamp
	Event struct {
		Timestamp time.Time `json:"timestamp"`
		Message
		Sequence int64 `json:"sequence"`
		Raised   bool  `json:"-"`
	}

	// Message is a typed payload associated with an aggregate
	Message struct {
		value any

		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data,omitempty"`

		mu sync.RWMutex
	}

	// EventType identifies the kind of an Event or Message
	EventType string

	// Empty is a payload with no fields
	Empty struct{}
)

// GetValue unmarshals the message data into the requested type. It reuses a
// cached value when the requested type matches, and otherwise unmarshals
// without replacing the cached type. This is safe for concurrent access
func (m *Message) GetValue[T any]() (T, error) {
	m.mu.RLock()
	if val, ok := m.value.(T); ok {
		m.mu.RUnlock()
		return val, nil
	}
	m.mu.RUnlock()
	return m.resolveValue[T]()
}

func (m *Message) resolveValue[T any]() (T, error) {
	m.mu.Lock()
	val, ok := m.value.(T)
	m.mu.Unlock()

	if ok {
		return val, nil
	}

	data, err := unmarshalMessageValue[T](m.Data)
	if err != nil {
		return data, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value == nil {
		m.value = data
	}

	return data, nil
}

func unmarshalMessageValue[T any](data json.RawMessage) (T, error) {
	var res T
	if err := json.Unmarshal(data, &res); err != nil {
		var zero T
		return zero, err
	}
	return res, nil
}
