package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type ctxKeyJobName struct{}

var jobNameKey ctxKeyJobName = struct{}{}

func JobNameFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if name, ok := ctx.Value(jobNameKey).(string); ok {
		return name
	}
	return ""
}

func ExtractJobName(pathSegment string, getter func(name string) bool,
) Middleware {
	m := &JobNameEx{pathSegment: pathSegment, getter: getter}
	return m.middleware
}

type JobNameEx struct {
	pathSegment string
	getter      func(name string) bool
}

func (self *JobNameEx) middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		jobName := self.jobNameFrom(r)
		if jobName == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r.WithContext(self.context(r, jobName)))
	}
	return http.HandlerFunc(fn)
}

func (self *JobNameEx) jobNameFrom(r *http.Request) string {
	jobName := r.PathValue(self.pathSegment)
	if jobName == "" {
		getLogger(r).With(
			slog.String("path", r.URL.String()),
			slog.String("path_segment", self.pathSegment),
		).Error("job name not found in path")
		return ""
	}
	if !self.getter(jobName) {
		getLogger(r).With(slog.String(logging.JobField, jobName)).
			Error("job not found")
		return ""
	}
	return jobName
}

func (self *JobNameEx) context(r *http.Request, name string) context.Context {
	ctx := context.WithValue(r.Context(), jobNameKey, name)
	ctx = logging.WithLogger(ctx, getLogger(r).With(
		slog.String(logging.JobField, name)))
	return zfscmd.WithJobID(ctx, name)
}
