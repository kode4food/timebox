package timebox_test

import (
	"encoding/json"
	"testing"

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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := TestState{Count: 10, Last: "initial"}
		result := applier(initialState, event)

		if receivedState.Count != 10 || receivedState.Last != "initial" {
			t.Errorf("expected state {Count: 10, Last: initial}, got: %+v",
				receivedState)
		}

		if receivedData.Name != "test" || receivedData.Value != 42 {
			t.Errorf("expected data {Name: test, Value: 42}, got: %+v",
				receivedData)
		}

		if receivedEvent != event {
			t.Error("received event does not match original event")
		}

		if result.Count != 52 || result.Last != "test" {
			t.Errorf("expected result {Count: 52, Last: test}, got: %+v",
				result)
		}
	})

	t.Run("returns original state on invalid JSON", func(t *testing.T) {
		type TestData struct {
			Name string `json:"name"`
		}

		type TestState struct {
			Value int
		}

		applier := timebox.MakeApplier(
			func(state TestState, ev *timebox.Event, data TestData) TestState {
				t.Fatal("applier should not be called with invalid JSON")
				return state
			},
		)

		event := &timebox.Event{
			Type: "test.event",
			Data: []byte("invalid json"),
		}

		initialState := TestState{Value: 100}
		result := applier(initialState, event)

		// Should return original state unchanged
		if result.Value != 100 {
			t.Errorf("expected state to be unchanged (100), got: %d",
				result.Value)
		}
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

		if !called {
			t.Fatal("applier was not called")
		}

		if !result.Called {
			t.Error("expected result.Called to be true")
		}
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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := &TestState{Value: 10}
		result := applier(initialState, event)

		if result.Value != 15 {
			t.Errorf("expected result value 15, got: %d", result.Value)
		}

		// Ensure original state wasn't mutated
		if initialState.Value != 10 {
			t.Errorf(
				"original state should not be mutated, expected 10, got: %d",
				initialState.Value,
			)
		}
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

		jsonData, _ := json.Marshal(10)
		event := &timebox.Event{
			Type: "test.event",
			Data: jsonData,
		}

		initialState := TestState{Total: 5}
		result := applier(initialState, event)

		if result.Total != 15 {
			t.Errorf("expected result total 15, got: %d", result.Total)
		}
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

		// Test increment
		incData, _ := json.Marshal(IncrementData{Delta: 5})
		event1 := &timebox.Event{Type: "increment", Data: incData}
		state := &TestState{Value: 10}
		state = appliers["increment"](state, event1)

		if state.Value != 15 {
			t.Errorf("expected value 15 after increment, got: %d", state.Value)
		}

		// Test reset
		event2 := &timebox.Event{Type: "reset", Data: []byte("{}")}
		state = appliers["reset"](state, event2)

		if state.Value != 0 {
			t.Errorf("expected value 0 after reset, got: %d", state.Value)
		}
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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type:     "test.event",
			Sequence: 42,
			Data:     jsonData,
		}

		result := applier(TestState{}, event)

		if result.EventType != "test.event" {
			t.Errorf("expected event type 'test.event', got: %s",
				result.EventType)
		}

		if result.EventSequence != 42 {
			t.Errorf("expected sequence 42, got: %d", result.EventSequence)
		}
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
		jsonData, _ := json.Marshal(data)
		event := &timebox.Event{
			Type: "user.updated",
			Data: jsonData,
		}

		result := applier(TestState{}, event)

		if result.UserCity != "Boston" {
			t.Errorf("expected city 'Boston', got: %s", result.UserCity)
		}
	})
}
