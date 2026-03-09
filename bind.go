package timebox

// Handler processes a single Event
type Handler func(*Event) error

// MakeHandler decodes event data into the provided type before invoking fn. It
// reuses the Event's cached value when available
func MakeHandler[T any](fn func(ev *Event, data T) error) Handler {
	return func(ev *Event) error {
		data, err := GetEventValue[T](ev)
		if err != nil {
			return err
		}
		return fn(ev, data)
	}
}

// MakeApplier wraps a strongly typed applier that receives the event payload
// value and returns an Applier that works with Event
func MakeApplier[T, Data any](fn func(T, *Event, Data) T) Applier[T] {
	return func(val T, ev *Event) T {
		data, err := GetEventValue[Data](ev)
		if err != nil {
			return val
		}
		return fn(val, ev, data)
	}
}

// MakeDispatcher routes events to handlers keyed by EventType, ignoring
// unmatched event types
func MakeDispatcher(handlers map[EventType]Handler) Handler {
	return func(ev *Event) error {
		if fn, ok := handlers[ev.Type]; ok {
			return fn(ev)
		}
		return nil
	}
}
