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
