package timebox

import "encoding/json"

type Handler func(*Event) error

func MakeHandler[T any](fn func(ev *Event, data T) error) Handler {
	return func(ev *Event) error {
		var data T
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			return err
		}
		return fn(ev, data)
	}
}

func MakeDispatcher(handlers map[EventType]Handler) Handler {
	return func(ev *Event) error {
		if fn, ok := handlers[ev.Type]; ok {
			return fn(ev)
		}
		return nil
	}
}
