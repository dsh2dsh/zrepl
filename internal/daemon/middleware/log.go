package middleware

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

func RequestLogger(opts ...LoggerOption) Middleware {
	l := &LogReq{
		levels:         make(map[string]logger.Level, 1),
		completedLevel: logger.Debug,
	}

	for _, fn := range opts {
		fn(l)
	}
	return l.middleware
}

type LoggerOption func(l *LogReq)

func WithCompletedInfo() LoggerOption {
	return func(self *LogReq) { self.completedLevel = logger.Info }
}

func WithCustomLevel(url string, level logger.Level) LoggerOption {
	return func(self *LogReq) { self.WithCustomLevel(url, level) }
}

type LogReq struct {
	levels map[string]logger.Level

	completedLevel logger.Level
}

func (self *LogReq) WithCustomLevel(url string, level logger.Level) *LogReq {
	self.levels[url] = level
	return self
}

func (self *LogReq) middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		log := getLogger(r)
		logLevel := self.requestLevel(r)

		methodURL := r.Method + " " + r.URL.String()
		log.WithField("proto", r.Proto).Log(logLevel, "\""+methodURL+"\"")
		log = log.WithField("req", methodURL)

		if next == nil {
			log.Error("no next handler configured")
			return
		}

		t := time.Now()
		next.ServeHTTP(w, r)
		log.WithField("duration", time.Since(t)).Log(
			min(logLevel, self.completedLevel), "request completed")
	}
	return http.HandlerFunc(fn)
}

func (self *LogReq) requestLevel(r *http.Request) logger.Level {
	if level, ok := self.levels[r.URL.String()]; ok {
		return level
	}
	return logger.Info
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
