package timebox

// Handler processes a single Event
type Handler func(*Event) error

// MakeHandler decodes event data into the provided type before invoking fn
// It reuses the Event's cached value when available
func MakeHandler[T any](fn func(ev *Event, data T) error) Handler {
	return func(ev *Event) error {
		var data T
		if err := ev.getValue(&data); err != nil {
			return err
		}
		return fn(ev, data)
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
