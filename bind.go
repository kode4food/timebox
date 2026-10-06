package timebox

// Handler processes a single Message
type Handler func(*Message) error

// MakeHandler decodes message data into the provided type before invoking fn
func MakeHandler[T any](fn func(msg *Message, data T) error) Handler {
	return func(msg *Message) error {
		data, err := msg.GetValue[T]()
		if err != nil {
			return err
		}
		return fn(msg, data)
	}
}

// MakeApplier wraps a strongly typed applier that receives the event payload
// value and returns an Applier that works with Event
func MakeApplier[T, Data any](fn func(T, *Event, Data) T) Applier[T] {
	return func(val T, ev *Event) T {
		data, err := ev.GetValue[Data]()
		if err != nil {
			return val
		}
		return fn(val, ev, data)
	}
}

// MakeDispatcher routes messages to handlers by type, ignoring unmatched types
func MakeDispatcher(handlers map[EventType]Handler) Handler {
	return func(msg *Message) error {
		if fn, ok := handlers[msg.Type]; ok {
			return fn(msg)
		}
		return nil
	}
}
