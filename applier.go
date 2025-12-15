package timebox

type (
	// Applier applies an event to an aggregate state, returning the new state
	Applier[T any] func(T, *Event) T

	// Appliers is a map of EventType to Applier for a given aggregate
	Appliers[T any] map[EventType]Applier[T]
)

// MakeApplier wraps a strongly typed applier that receives the event payload
// value and returns an Applier that works with Event
func MakeApplier[T, Data any](fn func(T, *Event, Data) T) Applier[T] {
	return func(val T, ev *Event) T {
		var data Data
		if err := ev.getValue(&data); err != nil {
			return val
		}
		return fn(val, ev, data)
	}
}
