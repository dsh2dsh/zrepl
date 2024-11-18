package nanosleep

import (
	"time"

	"golang.org/x/sys/unix"
)

// See https://www.reddit.com/r/golang/comments/jeqmtt/wake_up_at_time/
func SleepUntil(t time.Time) error {
	rqtp, err := unix.TimeToTimespec(t)
	if err != nil {
		return err
	}

	for {
		err := clockNanosleep(unix.CLOCK_REALTIME, unix.TIMER_ABSTIME, &rqtp, nil)
		if err != nil {
			if err == unix.EINTR { //nolint:errorlint // never wrapped
				continue
			}
			return err
		}
		return nil
	}
}

func NewTimer(d time.Duration) *Timer {
	t := new(Timer)
	t.Reset(d)
	return t
}

type Timer struct {
	ch <-chan time.Time
}

func (self *Timer) run(t time.Time, c chan<- time.Time) {
	_ = SleepUntil(t)
	c <- time.Now()
}

func (self *Timer) C() <-chan time.Time { return self.ch }

func (self *Timer) Reset(d time.Duration) bool {
	// It just starts a new goroutine without stopping existsing one, because Stop
	// does nothing. Yes, it's a goroutine leak, but I don't know how to stop
	// clock_nanosleep.
	wasRunning := self.Stop()
	c := make(chan time.Time, 1)
	self.ch = c
	go self.run(time.Now().Add(d), c)
	return wasRunning
}

// Stop does nothing, because I don't know how to stop clock_nanosleep.
func (self *Timer) Stop() bool { return true }
