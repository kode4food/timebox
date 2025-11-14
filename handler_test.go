package timebox_test

import (
	"encoding/json"
	"errors"
	"testing"

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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err := handler(event)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if !called {
			t.Fatal("handler was not called")
		}

		if receivedData.Name != "test" || receivedData.Value != 42 {
			t.Errorf("expected data {Name: test, Value: 42}, got: %+v",
				receivedData)
		}

		if receivedEvent != event {
			t.Error("received event does not match original event")
		}
	})

	t.Run("returns error on invalid JSON", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		handler := timebox.MakeHandler(
			func(ev *timebox.Event, data TestData) error {
				t.Fatal("handler should not be called with invalid JSON")
				return nil
			},
		)

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		err := handler(event)
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err := handler(event)
		if err != expectedErr {
			t.Errorf("expected error %v, got: %v", expectedErr, err)
		}
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
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if !called {
			t.Fatal("handler was not called")
		}
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

		// Call handler 1
		err := dispatcher(&timebox.Event{Type: "event.type1"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !handler1Called {
			t.Error("handler1 was not called")
		}
		if handler2Called {
			t.Error("handler2 should not have been called")
		}

		// Reset and call handler 2
		handler1Called = false
		handler2Called = false

		err = dispatcher(&timebox.Event{Type: "event.type2"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if handler1Called {
			t.Error("handler1 should not have been called")
		}
		if !handler2Called {
			t.Error("handler2 was not called")
		}
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

		// This should not error, just be ignored
		err := dispatcher(&timebox.Event{Type: "event.unknown"})
		if err != nil {
			t.Fatalf("expected no error for unmapped event, got: %v", err)
		}
		if handlerCalled {
			t.Error("handler should not have been called for unmapped event")
		}
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
		if err != expectedErr {
			t.Errorf("expected error %v, got: %v", expectedErr, err)
		}
	})

	t.Run("handles empty handler map", func(t *testing.T) {
		dispatcher := timebox.MakeDispatcher(
			map[timebox.EventType]timebox.Handler{},
		)

		// Should not panic or error
		err := dispatcher(&timebox.Event{Type: "any.event"})
		if err != nil {
			t.Fatalf("expected no error for empty dispatcher, got: %v", err)
		}
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
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedEvent != originalEvent {
			t.Error("received event does not match original event")
		}
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

		// Dispatch user created event
		userData, _ := json.Marshal(
			UserCreated{UserID: "user123", Email: "test@example.com"},
		)
		err := dispatcher(&timebox.Event{Type: "user.created", Data: userData})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if !userCreatedCalled {
			t.Error("user.created handler was not called")
		}
		if receivedUserID != "user123" {
			t.Errorf("expected userID user123, got: %s", receivedUserID)
		}

		// Dispatch order placed event
		orderData, _ := json.Marshal(
			OrderPlaced{OrderID: "order456", Amount: 100},
		)
		err = dispatcher(&timebox.Event{Type: "order.placed", Data: orderData})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if !orderPlacedCalled {
			t.Error("order.placed handler was not called")
		}
		if receivedAmount != 100 {
			t.Errorf("expected amount 100, got: %d", receivedAmount)
		}

		// Dispatch unknown event (should be silently ignored)
		err = dispatcher(&timebox.Event{
			Type: "unknown.event",
			Data: []byte("{}"),
		})
		if err != nil {
			t.Fatalf("expected no error for unknown event, got: %v", err)
		}
	})
}
