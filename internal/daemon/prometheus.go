package daemon

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/rpc/dataconn/frameconn"
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
	trace.RegisterMetrics(registerer)
	endpoint.RegisterMetrics(registerer)

	registerer.MustRegister(metricLogEntries)
	if err := zfs.PrometheusRegister(registerer); err != nil {
		panic(err)
	} else if err := frameconn.PrometheusRegister(registerer); err != nil {
		panic(err)
	}
}

func metricsEndpoints(mux *http.ServeMux, m ...middleware.Middleware) {
	mux.Handle(endpointMetrics, middleware.AppendHandler(m, promhttp.Handler()))
}

// --------------------------------------------------

type prometheusJobOutlet struct{}

var _ logger.Outlet = prometheusJobOutlet{}

func newPrometheusLogOutlet() prometheusJobOutlet {
	return prometheusJobOutlet{}
}

func (o prometheusJobOutlet) WriteEntry(entry logger.Entry) error {
	jobFieldVal, ok := entry.Fields[logging.JobField].(string)
	if !ok {
		jobFieldVal = "_nojobid"
	}
	metricLogEntries.WithLabelValues(jobFieldVal, entry.Level.String()).Inc()
	return nil
}
