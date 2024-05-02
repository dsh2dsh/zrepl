package zfscmd

import (
	"errors"
	"os/exec"
	"time"
)

// Implementation Note:
//
// Pre-events logged with debug
// Post-event without error logged with debug
// Post-events with error logged with error
// (Not all errors we observe at this layer) are actual errors in higher-level layers)

func startPreLogging(c *Cmd, _ time.Time) {
	c.log().Debug("starting command")
}

func startPostLogging(c *Cmd, err error, _ time.Time) {
	if err == nil {
		c.log().Info("started command")
	} else {
		c.log().WithError(err).Error("cannot start command")
	}
}

func waitPreLogging(c *Cmd, _ time.Time) {
	c.log().Debug("start waiting")
}

func waitPostLogging(c *Cmd, err error, debug bool) {
	log := c.log().
		WithField("total_time_s", c.usage.total_secs).
		WithField("systemtime_s", c.usage.system_secs).
		WithField("usertime_s", c.usage.user_secs)

	if err == nil {
		log.Debug("command exited without error")
		return
	}
	log = log.WithError(err)

	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		log = log.WithField("status", exitError.ExitCode())
	}
	if len(c.stdoutStderr) > 0 {
		log = log.WithField("stderr", string(c.stdoutStderr))
	}

	if debug {
		log.Debug("command exited with error")
	} else {
		log.Error("command exited with error")
	}
}
