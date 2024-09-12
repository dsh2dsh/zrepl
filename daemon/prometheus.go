package daemon

import (
	"context"
	"net"
	"net/http"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/rpc/dataconn/frameconn"
	"github.com/dsh2dsh/zrepl/util/tcpsock"
	"github.com/dsh2dsh/zrepl/zfs"
)

type prometheusJob struct {
	listen   string
	freeBind bool
	shutdown context.CancelFunc
}

func newPrometheusJobFromConfig(in *config.PrometheusMonitoring) (*prometheusJob, error) {
	if _, _, err := net.SplitHostPort(in.Listen); err != nil {
		return nil, err
	}
	return &prometheusJob{listen: in.Listen, freeBind: in.ListenFreeBind}, nil
}

var prom struct {
	taskLogEntries *prometheus.CounterVec
}

func init() {
	prom.taskLogEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zrepl",
		Subsystem: "daemon",
		Name:      "log_entries",
		Help:      "number of log entries per job task and level",
	}, []string{"zrepl_job", "level"})
	prometheus.MustRegister(prom.taskLogEntries)
}

func (j *prometheusJob) Name() string { return jobNamePrometheus }

func (j *prometheusJob) Status() *job.Status { return &job.Status{Type: job.TypeInternal} }

func (j *prometheusJob) OwnedDatasetSubtreeRoot() (p *zfs.DatasetPath, ok bool) { return nil, false }

func (j *prometheusJob) SenderConfig() *endpoint.SenderConfig { return nil }

func (j *prometheusJob) RegisterMetrics(registerer prometheus.Registerer) {}

func (j *prometheusJob) Run(ctx context.Context, cron *cron.Cron) {
	ctx, j.shutdown = context.WithCancel(ctx)
	defer j.shutdown()

	if err := zfs.PrometheusRegister(prometheus.DefaultRegisterer); err != nil {
		panic(err)
	}

	if err := frameconn.PrometheusRegister(prometheus.DefaultRegisterer); err != nil {
		panic(err)
	}

	log := logging.GetLogger(ctx, logging.SubsysInternal)

	l, err := tcpsock.Listen(j.listen, j.freeBind)
	if err != nil {
		log.WithError(err).Error("cannot listen")
		return
	}
	go func() {
		<-ctx.Done()
		l.Close()
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	err = http.Serve(l, mux)
	if err != nil && ctx.Err() == nil {
		log.WithError(err).Error("error while serving")
	}
}

func (j *prometheusJob) Shutdown() { j.shutdown() }

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
	prom.taskLogEntries.WithLabelValues(jobFieldVal, entry.Level.String()).Inc()
	return nil
}
