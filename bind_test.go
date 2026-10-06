package timebox_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type (
	nameValueData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	nameData struct {
		Name string `json:"name"`
	}

	emptyData struct{}

	userCreatedData struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
	}

	orderPlacedData struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}

	deltaData struct {
		Delta int `json:"delta"`
	}

	countState struct {
		Count int
		Last  string
	}

	valueState struct {
		Value int
	}

	calledState struct {
		Called bool
	}

	totalState struct {
		Total int
	}

	eventState struct {
		EventType     timebox.EventType
		EventSequence int64
	}

	addressData struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}

	userData struct {
		Name    string      `json:"name"`
		Age     int         `json:"age"`
		Address addressData `json:"address"`
	}

	cityState struct {
		UserCity string
	}
)

func TestMakeHandler(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		var called bool
		var receivedData nameValueData
		var receivedMessage *timebox.Message

		handler := timebox.MakeHandler(
			func(msg *timebox.Message, data nameValueData) error {
				called = true
				receivedData = data
				receivedMessage = msg
				return nil
			},
		)

		data := nameValueData{Name: "test", Value: 42}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		msg := &timebox.Message{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(msg)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, called)
		assert.Equal(t, nameValueData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, msg, receivedMessage)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		var called bool
		handler := timebox.MakeHandler(
			func(msg *timebox.Message, data nameData) error {
				called = true
				return nil
			},
		)

		msg := &timebox.Message{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		err := handler(msg)
		assert.Error(t, err)
		assert.False(t, called)
	})

	t.Run("handler error", func(t *testing.T) {
		expectedErr := errors.New("handler error")
		handler := timebox.MakeHandler(
			func(msg *timebox.Message, data nameData) error {
				return expectedErr
			},
		)

		data := nameData{Name: "test"}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		msg := &timebox.Message{
			Type: "test.event",
			Data: jsonData,
		}

		err = handler(msg)
		assert.Same(t, expectedErr, err)
	})

	t.Run("empty data", func(t *testing.T) {
		var called bool
		handler := timebox.MakeHandler(
			func(msg *timebox.Message, data emptyData) error {
				called = true
				return nil
			},
		)

		msg := &timebox.Message{
			Type: "test.event",
			Data: []byte("{}"),
		}

		err := handler(msg)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, called)
	})
}

func TestMakeDispatcher(t *testing.T) {
	t.Run("matching type", func(t *testing.T) {
		var handler1Called, handler2Called bool

		handlers := map[timebox.EventType]timebox.Handler{
			"event.type1": func(msg *timebox.Message) error {
				handler1Called = true
				return nil
			},
			"event.type2": func(msg *timebox.Message) error {
				handler2Called = true
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Message{Type: "event.type1"})
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, handler1Called)
		assert.False(t, handler2Called)

		handler1Called = false
		handler2Called = false

		err = dispatcher(&timebox.Message{Type: "event.type2"})
		if !assert.NoError(t, err) {
			return
		}
		assert.False(t, handler1Called)
		assert.True(t, handler2Called)
	})

	t.Run("unknown type", func(t *testing.T) {
		var handlerCalled bool

		handlers := map[timebox.EventType]timebox.Handler{
			"event.known": func(msg *timebox.Message) error {
				handlerCalled = true
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Message{Type: "event.unknown"})
		if !assert.NoError(t, err) {
			return
		}
		assert.False(t, handlerCalled)
	})

	t.Run("handler error", func(t *testing.T) {
		expectedErr := errors.New("handler error")

		handlers := map[timebox.EventType]timebox.Handler{
			"event.error": func(msg *timebox.Message) error {
				return expectedErr
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		err := dispatcher(&timebox.Message{Type: "event.error"})
		assert.Same(t, expectedErr, err)
	})

	t.Run("empty map", func(t *testing.T) {
		dispatcher := timebox.MakeDispatcher(
			map[timebox.EventType]timebox.Handler{},
		)

		err := dispatcher(&timebox.Message{Type: "any.event"})
		assert.NoError(t, err)
	})

	t.Run("message identity", func(t *testing.T) {
		var received *timebox.Message
		expected := []byte(`{"key": "value"}`)

		handlers := map[timebox.EventType]timebox.Handler{
			"event.test": func(msg *timebox.Message) error {
				received = msg
				return nil
			},
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		msg := &timebox.Message{
			Type: "event.test",
			Data: expected,
		}

		err := dispatcher(msg)
		if !assert.NoError(t, err) {
			return
		}
		assert.Same(t, msg, received)
	})
}

func TestHandlerDispatcher(t *testing.T) {
	t.Run("event and scheduled", func(t *testing.T) {
		var userCreatedCalled bool
		var orderPlacedCalled bool
		var receivedUserID string
		var receivedAmount int

		handlers := map[timebox.EventType]timebox.Handler{
			"user.created": timebox.MakeHandler(
				func(msg *timebox.Message, data userCreatedData) error {
					userCreatedCalled = true
					receivedUserID = data.UserID
					return nil
				},
			),
			"order.placed": timebox.MakeHandler(
				func(msg *timebox.Message, data orderPlacedData) error {
					orderPlacedCalled = true
					receivedAmount = data.Amount
					return nil
				},
			),
		}

		dispatcher := timebox.MakeDispatcher(handlers)

		userData, err := json.Marshal(
			userCreatedData{UserID: "user123", Email: "test@example.com"},
		)
		assert.NoError(t, err)
		ev := &timebox.Event{Type: "user.created", Data: userData}
		err = dispatcher(&ev.Message)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, userCreatedCalled)
		assert.Equal(t, "user123", receivedUserID)

		orderData, err := json.Marshal(
			orderPlacedData{OrderID: "order456", Amount: 100},
		)
		assert.NoError(t, err)
		msg := &timebox.Message{Type: "order.placed", Data: orderData}
		err = dispatcher(msg)
		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, orderPlacedCalled)
		assert.Equal(t, 100, receivedAmount)

		err = dispatcher(&timebox.Message{
			Type: "unknown.event",
			Data: []byte("{}"),
		})
		assert.NoError(t, err)
	})
}

func TestHandlerCache(t *testing.T) {
	handler := timebox.MakeHandler(
		func(msg *timebox.Message, data nameData) error {
			assert.Equal(t, "cached", data.Name)
			return nil
		},
	)

	msg := &timebox.Message{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	err := handler(msg)
	assert.NoError(t, err)

	err = handler(msg)
	assert.NoError(t, err)
}

func TestHandlerCacheType(t *testing.T) {
	structHandler := timebox.MakeHandler(
		func(msg *timebox.Message, data nameData) error {
			assert.Equal(t, "cached", data.Name)
			return nil
		},
	)
	mapHandler := timebox.MakeHandler(
		func(msg *timebox.Message, data map[string]any) error {
			assert.Equal(t, "cached", data["name"])
			return nil
		},
	)

	msg := &timebox.Message{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	assert.NoError(t, structHandler(msg))
	assert.NoError(t, mapHandler(msg))
	msg.Data = []byte("not json")
	assert.NoError(t, structHandler(msg))
	assert.Error(t, mapHandler(msg))
}

func TestMakeApplier(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		var receivedState countState
		var receivedData nameValueData
		var receivedEvent *timebox.Event

		applier := timebox.MakeApplier(
			func(
				state countState, ev *timebox.Event, data nameValueData,
			) countState {
				receivedState = state
				receivedData = data
				receivedEvent = ev
				return countState{
					Count: state.Count + data.Value,
					Last:  data.Name,
				}
			},
		)

		data := nameValueData{Name: "test", Value: 42}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := countState{Count: 10, Last: "initial"}
		res := applier(st, ev)

		assert.Equal(t, countState{Count: 10, Last: "initial"}, receivedState)
		assert.Equal(t, nameValueData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, ev, receivedEvent)
		assert.Equal(t, countState{Count: 52, Last: "test"}, res)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		var called bool
		applier := timebox.MakeApplier(
			func(
				state valueState, ev *timebox.Event, data nameData,
			) valueState {
				called = true
				return state
			},
		)

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		st := valueState{Value: 100}
		res := applier(st, ev)

		assert.False(t, called)
		assert.Equal(t, 100, res.Value)
	})

	t.Run("empty data", func(t *testing.T) {
		var called bool
		applier := timebox.MakeApplier(
			func(
				state calledState, ev *timebox.Event, data emptyData,
			) calledState {
				called = true
				return calledState{Called: true}
			},
		)

		ev := &timebox.Event{
			Type: "test.event",
			Data: []byte("{}"),
		}

		st := calledState{Called: false}
		res := applier(st, ev)

		assert.True(t, called)
		assert.True(t, res.Called)
	})

	t.Run("value state", func(t *testing.T) {
		applier := timebox.MakeApplier(
			func(
				st valueState, ev *timebox.Event, data deltaData,
			) valueState {
				st.Value += data.Delta
				return st
			},
		)

		data := deltaData{Delta: 5}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := valueState{Value: 10}
		res := applier(st, ev)

		assert.Equal(t, 15, res.Value)
		assert.Equal(t, 10, st.Value)
	})

	t.Run("primitive data", func(t *testing.T) {
		applier := timebox.MakeApplier(
			func(state totalState, ev *timebox.Event, delta int) totalState {
				return totalState{Total: state.Total + delta}
			},
		)

		jsonData, err := json.Marshal(10)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		st := totalState{Total: 5}
		res := applier(st, ev)

		assert.Equal(t, 15, res.Total)
	})

	t.Run("applier map", func(t *testing.T) {
		appliers := timebox.Appliers[valueState]{
			"increment": timebox.MakeApplier(
				func(
					st valueState, ev *timebox.Event, data deltaData,
				) valueState {
					st.Value += data.Delta
					return st
				},
			),
			"reset": timebox.MakeApplier(
				func(
					state valueState, ev *timebox.Event, _ struct{},
				) valueState {
					return valueState{Value: 0}
				},
			),
		}

		incData, err := json.Marshal(deltaData{Delta: 5})
		assert.NoError(t, err)
		event1 := &timebox.Event{Type: "increment", Data: incData}
		st := valueState{Value: 10}
		st = appliers["increment"](st, event1)

		assert.Equal(t, 15, st.Value)

		event2 := &timebox.Event{Type: "reset", Data: []byte("{}")}
		st = appliers["reset"](st, event2)

		assert.Equal(t, 0, st.Value)
	})

	t.Run("metadata", func(t *testing.T) {
		applier := timebox.MakeApplier(
			func(
				state eventState, ev *timebox.Event, data nameData,
			) eventState {
				return eventState{
					EventType:     ev.Type,
					EventSequence: ev.Sequence,
				}
			},
		)

		data := nameData{Name: "test"}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		ev := &timebox.Event{
			Type:     "test.event",
			Sequence: 42,
			Data:     jsonData,
		}

		res := applier(eventState{}, ev)

		assert.Equal(t, timebox.EventType("test.event"), res.EventType)
		assert.Equal(t, int64(42), res.EventSequence)
	})

	t.Run("nested data", func(t *testing.T) {
		applier := timebox.MakeApplier(
			func(state cityState, ev *timebox.Event, data userData) cityState {
				return cityState{UserCity: data.Address.City}
			},
		)

		data := userData{
			Name: "John",
			Age:  30,
			Address: addressData{
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

		res := applier(cityState{}, ev)

		assert.Equal(t, "Boston", res.UserCity)
	})
}

func TestApplierCache(t *testing.T) {
	applier := timebox.MakeApplier(
		func(state valueState, _ *timebox.Event, data deltaData) valueState {
			return valueState{Value: state.Value + data.Delta}
		},
	)

	jsonData, err := json.Marshal(deltaData{Delta: 3})
	assert.NoError(t, err)

	ev := &timebox.Event{
		Type: "test.event",
		Data: jsonData,
	}

	st := valueState{Value: 1}
	st = applier(st, ev)
	assert.Equal(t, 4, st.Value)

	ev.Data = []byte("not json")
	st = applier(st, ev)
	assert.Equal(t, 7, st.Value)
}
