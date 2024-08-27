package middleware

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/logger"
)

type PromControl struct {
	RequestBegin    *prometheus.CounterVec
	RequestFinished *prometheus.HistogramVec
}

func genRequestId() uint64 {
	return atomic.AddUint64(&nextRequestId, 1)
}

var nextRequestId uint64

func RequestLogger(log logger.Logger, prom *PromControl) Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			log = log.WithField("request_id", genRequestId())
			msg := "\"" + r.Method + " " + r.URL.String() + "\""
			log.Info(msg)

			prom.RequestBegin.WithLabelValues(r.URL.Path).Inc()
			defer prometheus.
				NewTimer(prom.RequestFinished.WithLabelValues(r.URL.Path)).
				ObserveDuration()

			if next == nil {
				log.WithField("method", r.Method).WithField("url", r.URL).
					Error("no next handler configured")
				return
			}

			t := time.Now()
			next.ServeHTTP(w, r)
			log.WithField("duration", time.Since(t)).Info(msg)
		}
		return http.HandlerFunc(fn)
	}
}
