package daemon

import (
	"context"
	"log/slog"
	"time"

	"github.com/dsh2dsh/cron/v3"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/daemon/nanosleep"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func newCron(ctx context.Context, verbose bool) *cron.Cron {
	log := logging.GetLogger(ctx, logging.SubsysCron)
	return cron.New(
		cron.WithLogger(newCronLogger(log, verbose)),
		cron.WithTimer(func(d time.Duration) cron.Timer {
			return nanosleep.NewTimer(d)
		}),
	)
}

func newCronLogger(log *slog.Logger, verbose bool) *cronLogger {
	return &cronLogger{log: log, verbose: verbose}
}

type cronLogger struct {
	log     *slog.Logger
	verbose bool
}

// Info logs routine messages about cron's operation.
func (self *cronLogger) Info(msg string, keysAndValues ...any) {
	if self.verbose {
		self.log.With(keysAndValues...).Info(msg)
	}
}

// Error logs an error condition.
func (self *cronLogger) Error(err error, msg string, keysAndValues ...any) {
	logger.WithError(self.log.With(keysAndValues...), err, msg)
}
