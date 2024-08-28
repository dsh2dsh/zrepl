package middleware

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/logger"
)

type PromControl struct {
	RequestBegin    *prometheus.CounterVec
	RequestFinished *prometheus.HistogramVec
}

func RequestLogger(log logger.Logger, prom *PromControl, opts ...loggerOption,
) Middleware {
	l := &requestLogger{
		log:  log,
		prom: prom,

		completedLevel: logger.Debug,
	}

	for _, fn := range opts {
		fn(l)
	}
	return l.middleware
}

type loggerOption func(l *requestLogger)

func WithCompletedInfo() loggerOption {
	return func(self *requestLogger) { self.completedLevel = logger.Info }
}

type requestLogger struct {
	log  logger.Logger
	prom *PromControl

	completedLevel logger.Level
}

func (self *requestLogger) middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		log := self.log
		if requestId := GetRequestId(r.Context()); requestId != "" {
			log = log.WithField("rid", requestId)
		}

		methodURL := r.Method + " " + r.URL.String()
		log.Info("\"" + methodURL + "\"")
		log = log.WithField("req", methodURL)

		self.prom.RequestBegin.WithLabelValues(r.URL.Path).Inc()
		defer prometheus.
			NewTimer(self.prom.RequestFinished.WithLabelValues(r.URL.Path)).
			ObserveDuration()

		if next == nil {
			log.Error("no next handler configured")
			return
		}

		t := time.Now()
		next.ServeHTTP(w, r)
		log.WithField("duration", time.Since(t)).
			Log(self.completedLevel, "request completed")
	}
	return http.HandlerFunc(fn)
}
