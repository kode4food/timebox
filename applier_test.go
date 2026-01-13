package timebox_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

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
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := TestState{Count: 10, Last: "initial"}
		result := applier(initialState, event)

		assert.Equal(t, TestState{Count: 10, Last: "initial"}, receivedState)
		assert.Equal(t, TestData{Name: "test", Value: 42}, receivedData)
		assert.Same(t, event, receivedEvent)
		assert.Equal(t, TestState{Count: 52, Last: "test"}, result)
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

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		initialState := TestState{Value: 100}
		result := applier(initialState, event)

		assert.False(t, called)
		assert.Equal(t, 100, result.Value)
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

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("{}"),
		}

		initialState := TestState{Called: false}
		result := applier(initialState, event)

		assert.True(t, called)
		assert.True(t, result.Called)
	})

	t.Run("handles pointer state types", func(t *testing.T) {
		type TestData struct {
			Delta int `json:"delta"`
		}

		type TestState struct {
			Value int
		}

		applier := timebox.MakeApplier(
			func(
				state *TestState, ev *timebox.Event, data TestData,
			) *TestState {
				result := *state
				result.Value += data.Delta
				return &result
			},
		)

		data := TestData{Delta: 5}
		jsonData, err := json.Marshal(data)
		assert.NoError(t, err)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := &TestState{Value: 10}
		result := applier(initialState, event)

		assert.Equal(t, 15, result.Value)
		assert.Equal(t, 10, initialState.Value)
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
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := TestState{Total: 5}
		result := applier(initialState, event)

		assert.Equal(t, 15, result.Total)
	})

	t.Run("works in Appliers map", func(t *testing.T) {
		type IncrementData struct {
			Delta int `json:"delta"`
		}

		type TestState struct {
			Value int
		}

		appliers := timebox.Appliers[*TestState]{
			"increment": timebox.MakeApplier(
				func(
					state *TestState, ev *timebox.Event, data IncrementData,
				) *TestState {
					result := *state
					result.Value += data.Delta
					return &result
				},
			),
			"reset": timebox.MakeApplier(
				func(
					state *TestState, ev *timebox.Event, _ struct{},
				) *TestState {
					return &TestState{Value: 0}
				},
			),
		}

		incData, err := json.Marshal(IncrementData{Delta: 5})
		assert.NoError(t, err)
		event1 := &timebox.Event{Type: "increment", Data: incData}
		state := &TestState{Value: 10}
		state = appliers["increment"](state, event1)

		assert.Equal(t, 15, state.Value)

		event2 := &timebox.Event{Type: "reset", Data: []byte("{}")}
		state = appliers["reset"](state, event2)

		assert.Equal(t, 0, state.Value)
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
		event := &timebox.Event{
			Type:     "test.event",
			Sequence: 42,
			Data:     jsonData,
		}

		result := applier(TestState{}, event)

		assert.Equal(t, timebox.EventType("test.event"), result.EventType)
		assert.Equal(t, int64(42), result.EventSequence)
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
		event := &timebox.Event{
			Type: "user.updated",
			Data: jsonData,
		}

		result := applier(TestState{}, event)

		assert.Equal(t, "Boston", result.UserCity)
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

	event := &timebox.Event{
		Type: "test.event",
		Data: jsonData,
	}

	state := TestState{Value: 1}
	state = applier(state, event)
	assert.Equal(t, 4, state.Value)

	event.Data = []byte("not json")
	state = applier(state, event)
	assert.Equal(t, 7, state.Value)
}
