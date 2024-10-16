package daemon

import (
	"context"

	"github.com/dsh2dsh/cron/v3"

	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
)

func newCron(ctx context.Context, verbose bool) *cron.Cron {
	log := logging.GetLogger(ctx, logging.SubsysCron)
	return cron.New(cron.WithLogger(newCronLogger(log, verbose)))
}

func newCronLogger(log logger.Logger, verbose bool) *cronLogger {
	return &cronLogger{log: log, verbose: verbose}
}

type cronLogger struct {
	log     logger.Logger
	verbose bool
}

// Info logs routine messages about cron's operation.
func (self *cronLogger) Info(msg string, keysAndValues ...any) {
	if self.verbose {
		self.withFields(keysAndValues).Info(msg)
	}
}

func (self *cronLogger) withFields(keysAndValues []any) logger.Logger {
	l := self.log
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		k, ok := keysAndValues[i].(string)
		if ok {
			l = l.WithField(k, keysAndValues[i+1])
		}
	}
	return l
}

// Error logs an error condition.
func (self *cronLogger) Error(err error, msg string, keysAndValues ...any) {
	self.withFields(keysAndValues).Error(msg)
}
