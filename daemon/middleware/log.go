package middleware

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/logger"
)

type PromControl struct {
	RequestBegin    *prometheus.CounterVec
	RequestFinished *prometheus.HistogramVec
}

func RequestLogger(log logger.Logger, prom *PromControl) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log := log.WithField("method", r.Method).WithField("uri", r.URL)
			log.Info("new request")

			prom.RequestBegin.WithLabelValues(r.URL.Path).Inc()
			defer prometheus.
				NewTimer(prom.RequestFinished.WithLabelValues(r.URL.Path)).
				ObserveDuration()

			if next != nil {
				next.ServeHTTP(w, r)
			} else {
				log.Error("no next handler configured")
			}
			log.Debug("request completed")
		})
	}
}
