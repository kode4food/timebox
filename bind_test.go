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
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(ev)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, called)
		assert.Equal(t, TestData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, ev, receivedEvent)
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

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		err := handler(ev)
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
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(ev)
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

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("{}"),
		}

		err := handler(ev)
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
		var received *timebox.Event
		expected := []byte(`{"key": "value"}`)

		handlers := map[timebox.EventType]timebox.Handler{
			"event.test": func(ev *timebox.Event) error {
				received = ev
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		ev := &timebox.Event{
			Type: "event.test",
			Data: expected,
		}

		err := dispatcher(ev)
		if !assert.NoError(t, err) {
			return
		}
		assert.Same(t, ev, received)
	})
}

func TestHandlerDispatcher(t *testing.T) {
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

	ev := &timebox.Event{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	err := handler(ev)
	assert.NoError(t, err)

	err = handler(ev)
	assert.NoError(t, err)
}

func TestHandlerCacheType(t *testing.T) {
	type CachedData struct {
		Name string `json:"name"`
	}

	structHandler := timebox.MakeHandler(
		func(ev *timebox.Event, data CachedData) error {
			assert.Equal(t, "cached", data.Name)
			return nil
		},
	)
	mapHandler := timebox.MakeHandler(
		func(ev *timebox.Event, data map[string]any) error {
			assert.Equal(t, "cached", data["name"])
			return nil
		},
	)

	ev := &timebox.Event{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	assert.NoError(t, structHandler(ev))
	assert.NoError(t, mapHandler(ev))
	ev.Data = []byte("not json")
	assert.NoError(t, structHandler(ev))
	assert.Error(t, mapHandler(ev))
}

func TestMakeApplier(t *testing.T) {
	t.Run("successfully unmarshals and calls applier", func(t *testing.T) {
		type TestData struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		type TestState struct {
			Count int
			Last  string
		}

		var receivedState TestState
		var receivedData TestData
		var receivedEvent *timebox.Event

		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data TestData) TestState {
				receivedState = state
				receivedData = data
				receivedEvent = ev
				return TestState{
					Count: state.Count + data.Value,
					Last:  data.Name,
				}
			},
		)

		data := TestData{Name: "test", Value: 42}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := TestState{Count: 10, Last: "initial"}
		res := applier(st, ev)

		assert.Equal(t, TestState{Count: 10, Last: "initial"}, receivedState)
		assert.Equal(t, TestData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, ev, receivedEvent)
		assert.Equal(t, TestState{Count: 52, Last: "test"}, res)
	})

	t.Run("returns original state on invalid JSON", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		type TestState struct {
			Value int
		}

		var called bool
		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data TestData) TestState {
				called = true
				return state
			},
		)

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		st := TestState{Value: 100}
		res := applier(st, ev)

		assert.False(t, called)
		assert.Equal(t, 100, res.Value)
	})

	t.Run("handles empty struct types", func(t *testing.T) {
		type EmptyData struct{}

		type TestState struct {
			Called bool
		}

		var called bool
		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data EmptyData) TestState {
				called = true
				return TestState{Called: true}
			},
		)

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("{}"),
		}

		st := TestState{Called: false}
		res := applier(st, ev)

		assert.True(t, called)
		assert.True(t, res.Called)
	})

	t.Run("handles pointer state types", func(t *testing.T) {
		type TestData struct {
			Delta int `json:"delta"`
		}

		type TestState struct {
			Value int
		}

		applier := timebox.MakeApplier(
			func(st TestState, ev *timebox.Event, data TestData) TestState {
				st.Value += data.Delta
				return st
			},
		)

		data := TestData{Delta: 5}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := TestState{Value: 10}
		res := applier(st, ev)

		assert.Equal(t, 15, res.Value)
		assert.Equal(t, 10, st.Value)
	})

	t.Run("handles primitive data types", func(t *testing.T) {
		type TestState struct {
			Total int
		}

		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, delta int) TestState {
				return TestState{Total: state.Total + delta}
			},
		)

		jsonData, err := json.Marshal(10)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := TestState{Total: 5}
		res := applier(st, ev)

		assert.Equal(t, 15, res.Total)
	})

	t.Run("works in Appliers map", func(t *testing.T) {
		type IncrementData struct {
			Delta int `json:"delta"`
		}

		type TestState struct {
			Value int
		}

		appliers := timebox.Appliers[TestState]{
			"increment": timebox.MakeApplier(
				func(
					st TestState, ev *timebox.Event, data IncrementData,
				) TestState {
					st.Value += data.Delta
					return st
				},
			),
			"reset": timebox.MakeApplier(
				func(
					state TestState, ev *timebox.Event, _ struct{},
				) TestState {
					return TestState{Value: 0}
				},
			),
		}

		incData, err := json.Marshal(IncrementData{Delta: 5})
		assert.NoError(t, err)
		event1 := &timebox.Event{Type: "increment", Data: incData}
		st := TestState{Value: 10}
		st = appliers["increment"](st, event1)

		assert.Equal(t, 15, st.Value)

		event2 := &timebox.Event{Type: "reset", Data: []byte("{}")}
		st = appliers["reset"](st, event2)

		assert.Equal(t, 0, st.Value)
	})

	t.Run("preserves event metadata", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		type TestState struct {
			EventType     timebox.EventType
			EventSequence int64
		}

		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data TestData) TestState {
				return TestState{
					EventType:     ev.Type,
					EventSequence: ev.Sequence,
				}
			},
		)

		data := TestData{Name: "test"}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type:     "test.event",
			Sequence: 42,
			Data:     jsonData,
		}

		res := applier(TestState{}, ev)

		assert.Equal(t, timebox.EventType("test.event"), res.EventType)
		assert.Equal(t, int64(42), res.EventSequence)
	})

	t.Run("handles complex nested data structures", func(t *testing.T) {
		type Address struct {
			Street string `json:"street"`
			City   string `json:"city"`
		}

		type UserData struct {
			Name    string  `json:"name"`
			Age     int     `json:"age"`
			Address Address `json:"address"`
		}

		type TestState struct {
			UserCity string
		}

		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data UserData) TestState {
				return TestState{UserCity: data.Address.City}
			},
		)

		data := UserData{
			Name: "John",
			Age:  30,
			Address: Address{
				Street: "Main St",
				City:   "Boston",
			},
		}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "user.updated",
			Data: jsonData,
		}

		res := applier(TestState{}, ev)

		assert.Equal(t, "Boston", res.UserCity)
	})
}

func TestApplierCache(t *testing.T) {
	type IncrementData struct {
		Delta int `json:"delta"`
	}

	type TestState struct {
		Value int
	}

	applier := timebox.MakeApplier(
		func(state TestState, _ *timebox.Event, data IncrementData) TestState {
			return TestState{Value: state.Value + data.Delta}
		},
	)

	jsonData, err := json.Marshal(IncrementData{Delta: 3})
	assert.NoError(t, err)

	ev := &timebox.Event{
		Type: "test.event",
		Data: jsonData,
	}

	st := TestState{Value: 1}
	st = applier(st, ev)
	assert.Equal(t, 4, st.Value)

	ev.Data = []byte("not json")
	st = applier(st, ev)
	assert.Equal(t, 7, st.Value)
}
