package zfscmd

import (
	"bufio"
	"bytes"
	"errors"
	"os/exec"
	"time"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

// Implementation Note:
//
// Pre-events logged with debug
// Post-event without error logged with debug
// Post-events with error logged with error
// (Not all errors we observe at this layer) are actual errors in higher-level layers)

func startPreLogging(c *Cmd, _ time.Time) {
	c.logWithCmd().Debug("starting command")
}

func startPostLogging(c *Cmd, err error, _ time.Time) {
	if err == nil {
		c.log().Info("\"" + c.String() + "\"")
	} else {
		c.logWithCmd().WithError(err).Error("cannot start command")
	}
}

func waitPreLogging(c *Cmd, _ time.Time) {
	c.logWithCmd().Debug("start waiting")
}

func waitPostLogging(c *Cmd, err error, debug bool) {
	log := c.logWithCmd().
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

	level := logger.Error
	if debug {
		level = logger.Debug
	}
	log.Log(level, "command exited with error")

	if len(c.stderrOutput) == 0 {
		return
	}

	s := bufio.NewScanner(bytes.NewReader(c.stderrOutput))
	for s.Scan() {
		c.log().Log(level, "output: "+s.Text())
	}
}
