package scheduler

import "time"

type (
	// Clock provides the current scheduler time
	Clock func() time.Time

	// Timer provides the resettable wakeup used by Scheduler
	Timer interface {
		Channel() <-chan time.Time
		Reset(time.Duration) bool
		Stop() bool
	}

	// TimerConstructor creates a Timer for a delay
	TimerConstructor func(time.Duration) Timer

	systemTimer struct {
		*time.Timer
	}
)

func (t *systemTimer) Channel() <-chan time.Time {
	return t.C
}

func newTimer(delay time.Duration) Timer {
	return &systemTimer{Timer: time.NewTimer(delay)}
}
