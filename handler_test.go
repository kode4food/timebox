package timebox_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestMakeHandler(t *testing.T) {
	t.Run("successfully unmarshals and calls handler", func(t *testing.T) {
		type TestData struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		var called bool
		var receivedData TestData
		var receivedEvent *timebox.Event

		handler := timebox.MakeHandler(
			func(ev *timebox.Event, data TestData) error {
				called = true
				receivedData = data
				receivedEvent = ev
				return nil
			},
		)

		data := TestData{Name: "test", Value: 42}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(event)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, called)
		assert.Equal(t, TestData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, event, receivedEvent)
	})

	t.Run("returns error on invalid JSON", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		var called bool
		handler := timebox.MakeHandler(
			func(ev *timebox.Event, data TestData) error {
				called = true
				return nil
			},
		)

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		err := handler(event)
		assert.Error(t, err)
		assert.False(t, called)
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		expectedErr := errors.New("handler error")
		handler := timebox.MakeHandler(
			func(ev *timebox.Event, data TestData) error {
				return expectedErr
			},
		)

		data := TestData{Name: "test"}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(event)
		assert.Same(t, expectedErr, err)
	})

	t.Run("handles empty struct types", func(t *testing.T) {
		type EmptyData struct{}

		var called bool
		handler := timebox.MakeHandler(
			func(ev *timebox.Event, data EmptyData) error {
				called = true
				return nil
			},
		)

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("{}"),
		}

		err := handler(event)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, called)
	})
}

func TestMakeDispatcher(t *testing.T) {
	t.Run("dispatches to correct handler", func(t *testing.T) {
		var handler1Called, handler2Called bool

		handlers := map[timebox.EventType]timebox.Handler{
			"event.type1": func(ev *timebox.Event) error {
				handler1Called = true
				return nil
			},
			"event.type2": func(ev *timebox.Event) error {
				handler2Called = true
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Event{Type: "event.type1"})
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, handler1Called)
		assert.False(t, handler2Called)

		handler1Called = false
		handler2Called = false

		err = dispatcher(&timebox.Event{Type: "event.type2"})
		if !assert.NoError(t, err) {
			return
		}
		assert.False(t, handler1Called)
		assert.True(t, handler2Called)
	})

	t.Run("ignores unmapped event types", func(t *testing.T) {
		var handlerCalled bool

		handlers := map[timebox.EventType]timebox.Handler{
			"event.known": func(ev *timebox.Event) error {
				handlerCalled = true
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Event{Type: "event.unknown"})
		if !assert.NoError(t, err) {
			return
		}
		assert.False(t, handlerCalled)
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		expectedErr := errors.New("handler error")

		handlers := map[timebox.EventType]timebox.Handler{
			"event.error": func(ev *timebox.Event) error {
				return expectedErr
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Event{Type: "event.error"})
		assert.Same(t, expectedErr, err)
	})

	t.Run("handles empty handler map", func(t *testing.T) {
		dispatcher := timebox.MakeDispatcher(
			map[timebox.EventType]timebox.Handler{},
		)

		err := dispatcher(&timebox.Event{Type: "any.event"})
		assert.NoError(t, err)
	})

	t.Run("passes correct event to handler", func(t *testing.T) {
		var receivedEvent *timebox.Event
		expectedData := []byte(`{"key": "value"}`)

		handlers := map[timebox.EventType]timebox.Handler{
			"event.test": func(ev *timebox.Event) error {
				receivedEvent = ev
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		originalEvent := &timebox.Event{
			Type: "event.test",
			Data: expectedData,
		}

		err := dispatcher(originalEvent)
		if !assert.NoError(t, err) {
			return
		}
		assert.Same(t, originalEvent, receivedEvent)
	})
}

func TestMakeHandlerWithDispatcher(t *testing.T) {
	t.Run("integration: typed handlers in dispatcher", func(t *testing.T) {
		type UserCreated struct {
			UserID string `json:"user_id"`
			Email  string `json:"email"`
		}

		type OrderPlaced struct {
			OrderID string `json:"order_id"`
			Amount  int    `json:"amount"`
		}

		var userCreatedCalled bool
		var orderPlacedCalled bool
		var receivedUserID string
		var receivedAmount int

		handlers := map[timebox.EventType]timebox.Handler{
			"user.created": timebox.MakeHandler(
				func(ev *timebox.Event, data UserCreated) error {
					userCreatedCalled = true
					receivedUserID = data.UserID
					return nil
				},
			),
			"order.placed": timebox.MakeHandler(
				func(ev *timebox.Event, data OrderPlaced) error {
					orderPlacedCalled = true
					receivedAmount = data.Amount
					return nil
				},
			),
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		userData, err := json.Marshal(
			UserCreated{UserID: "user123", Email: "test@example.com"},
		)
		assert.NoError(t, err)
		err = dispatcher(&timebox.Event{Type: "user.created", Data: userData})
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, userCreatedCalled)
		assert.Equal(t, "user123", receivedUserID)

		orderData, err := json.Marshal(
			OrderPlaced{OrderID: "order456", Amount: 100},
		)
		assert.NoError(t, err)
		err = dispatcher(&timebox.Event{Type: "order.placed", Data: orderData})
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, orderPlacedCalled)
		assert.Equal(t, 100, receivedAmount)

		err = dispatcher(&timebox.Event{
			Type: "unknown.event",
			Data: []byte("{}"),
		})
		assert.NoError(t, err)
	})
}

func TestHandlerCache(t *testing.T) {
	type TestData struct {
		Name string `json:"name"`
	}

	handler := timebox.MakeHandler(
		func(ev *timebox.Event, data TestData) error {
			assert.Equal(t, "cached", data.Name)
			return nil
		},
	)

	event := &timebox.Event{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	err := handler(event)
	assert.NoError(t, err)

	err = handler(event)
	assert.NoError(t, err)
}
