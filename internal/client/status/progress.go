package status

import "time"

type speed struct {
	time time.Time
	val  uint64

	lastChange time.Time
}

func (self *speed) Valid() bool { return !self.time.IsZero() }

func (self *speed) Update(v uint64) int64 {
	if !self.Valid() {
		self.time, self.val, self.lastChange = time.Now(), v, time.Now()
		return 0
	}

	if self.val != v {
		self.lastChange = time.Now()
	} else if time.Since(self.lastChange) > 3*time.Second {
		self.Reset()
		return 0
	}

	var delta int64
	if v >= self.val {
		delta = int64(v - self.val)
	} else {
		delta = -int64(self.val - v)
	}
	rate := float64(delta) / time.Since(self.time).Seconds()
	self.time, self.val = time.Now(), v
	return int64(rate)
}

func (self *speed) Reset() {
	self.time, self.val = time.Time{}, 0
}
