package timebox

type (
	Applier[T any]  func(T, *Event) T
	Appliers[T any] map[EventType]Applier[T]
)

func MakeApplier[T, Data any](fn func(T, *Event, Data) T) Applier[T] {
	return func(val T, ev *Event) T {
		var data Data
		if err := ev.getValue(&data); err != nil {
			return val
		}
		return fn(val, ev, data)
	}
}
