package daemon

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/version"
	"github.com/dsh2dsh/zrepl/internal/zfs"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

const endpointMetrics = "/metrics"

var metricLogEntries *prometheus.CounterVec

func init() {
	metricLogEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zrepl",
		Subsystem: "daemon",
		Name:      "log_entries",
		Help:      "number of log entries per job task and level",
	}, []string{"zrepl_job", "level"})
}

func mustRegisterMetrics(registerer prometheus.Registerer) {
	// register global (=non job-local) metrics
	version.PrometheusRegister(registerer)
	zfscmd.RegisterMetrics(registerer)
	endpoint.RegisterMetrics(registerer)

	registerer.MustRegister(metricLogEntries)
	if err := zfs.PrometheusRegister(registerer); err != nil {
		panic(err)
	}
}

func metricsEndpoints(mux *http.ServeMux, m ...middleware.Middleware) {
	mux.Handle(endpointMetrics, middleware.AppendHandler(m, promhttp.Handler()))
}

// --------------------------------------------------

type promLogOutlet struct {
	jobName string
}

var _ slog.Handler = (*promLogOutlet)(nil)

func newPrometheusLogOutlet() *promLogOutlet { return &promLogOutlet{} }

func (promLogOutlet) Enabled(context.Context, slog.Level) bool { return true }

func (self *promLogOutlet) Handle(_ context.Context, r slog.Record) error {
	jobName := self.jobName
	if jobName == "" {
		jobName = "_nojobid"
	}
	metricLogEntries.WithLabelValues(jobName, r.Level.String()).Inc()
	return nil
}

func (self *promLogOutlet) WithAttrs(attrs []slog.Attr) slog.Handler {
	for _, a := range attrs {
		if a.Key == logging.JobField {
			return &promLogOutlet{jobName: a.Value.String()}
		}
	}
	return self
}

func (self *promLogOutlet) WithGroup(name string) slog.Handler {
	if name == "" {
		return self
	}
	return self
}
