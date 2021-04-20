package shardx

import "time"

type simpleTimer struct {
	duration time.Duration
	timer    *time.Timer
}

var _ Timer = &simpleTimer{}

func (t *simpleTimer) Reset() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
	t.timer.Reset(t.duration)
}

func (t *simpleTimer) Stop() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

func (t *simpleTimer) Chan() <-chan time.Time {
	return t.timer.C
}

func defaultTimer(duration time.Duration) *simpleTimer {
	timer := time.NewTimer(1000 * time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	return &simpleTimer{
		duration: duration,
		timer:    timer,
	}
}

type wrappedTimer struct {
	running bool
	timer   Timer
}

func newWrappedTimer(timer Timer) *wrappedTimer {
	return &wrappedTimer{
		running: false,
		timer:   timer,
	}
}

func (t *wrappedTimer) stopIfRunning() {
	if t.running {
		t.running = false
		t.timer.Stop()
	}
}

func (t *wrappedTimer) reset() {
	t.running = true
	t.timer.Reset()
}

func (t *wrappedTimer) expired() {
	t.running = false
}

func (t *wrappedTimer) getChan() <-chan time.Time {
	return t.timer.Chan()
}
