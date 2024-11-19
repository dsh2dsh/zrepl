package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func RequestLogger(opts ...LoggerOption) Middleware {
	l := &LogReq{
		levels:         make(map[string]slog.Level, 1),
		completedLevel: slog.LevelDebug,
	}

	for _, fn := range opts {
		fn(l)
	}
	return l.middleware
}

type LoggerOption func(l *LogReq)

func WithCompletedInfo() LoggerOption {
	return func(self *LogReq) { self.completedLevel = slog.LevelInfo }
}

func WithCustomLevel(url string, level slog.Level) LoggerOption {
	return func(self *LogReq) { self.WithCustomLevel(url, level) }
}

type LogReq struct {
	levels map[string]slog.Level

	completedLevel slog.Level
}

func (self *LogReq) WithCustomLevel(url string, level slog.Level) *LogReq {
	self.levels[url] = level
	return self
}

func (self *LogReq) middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		log := getLogger(r).With(
			slog.String("proto", r.Proto),
			slog.String("remote_addr", r.RemoteAddr))

		logLevel := self.requestLevel(r)
		methodURL := r.Method + " " + r.URL.String()
		log.Log(r.Context(), logLevel, methodURL)
		log = log.With(slog.String("req", methodURL))

		if next == nil {
			log.Error("no next handler configured")
			return
		}

		t := time.Now()
		next.ServeHTTP(w, r)
		log.With(slog.Duration("duration", time.Since(t))).
			Log(r.Context(),
				min(logLevel, self.completedLevel), "request completed")
	}
	return http.HandlerFunc(fn)
}

func (self *LogReq) requestLevel(r *http.Request) slog.Level {
	if level, ok := self.levels[r.URL.String()]; ok {
		return level
	}
	return slog.LevelInfo
}

// --------------------------------------------------

func PrometheusMetrics(begin *prometheus.CounterVec,
	finished *prometheus.HistogramVec,
) Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			begin.WithLabelValues(r.URL.Path).Inc()
			defer prometheus.
				NewTimer(finished.WithLabelValues(r.URL.Path)).
				ObserveDuration()
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}
