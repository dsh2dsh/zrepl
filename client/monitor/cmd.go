package monitor

import (
	"context"
	"fmt"
	"iter"
	"runtime"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/client/status"
	"github.com/dsh2dsh/zrepl/config"
)

var (
	runningWarn time.Duration
	runningCrit time.Duration

	snapJob    string
	snapPrefix string
	snapCrit   time.Duration
	snapWarn   time.Duration

	countWarn uint
	countCrit uint

	maxProcs int
)

var Subcommand = &cli.Subcommand{
	Use:   "monitor",
	Short: "Icinga/Nagios health checks",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
	},

	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{aliveCmd, snapshotsCmd}
	},
}

var aliveCmd = &cli.Subcommand{
	Use:   "alive",
	Short: "check the daemon is alive",

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
		f := c.Flags()
		f.DurationVarP(&runningWarn, "warn", "w", 0, "warning job running time")
		f.DurationVarP(&runningCrit, "crit", "c", 0, "critical job running time")
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string) error {
		return withStatusClient(cmd, func(c *status.Client) error {
			return NewAliveCheck(c).WithThresholds(runningWarn, runningCrit).
				OutputAndExit()
		})
	},
}

var snapshotsCmd = &cli.Subcommand{
	Use:   "snapshots",
	Short: "check snapshots according to rules",

	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{countsCmd, latestCmd, oldestCmd}
	},

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
		f := c.PersistentFlags()
		f.StringVarP(&snapJob, "job", "j", "", "name of the job")
		f.StringVarP(&snapPrefix, "prefix", "p", "", "snapshot prefix")
		f.DurationVarP(&snapCrit, "crit", "c", 0, "critical snapshot age")
		f.DurationVarP(&snapWarn, "warn", "w", 0, "warning snapshot age")
		f.IntVarP(&maxProcs, "procs", "n", runtime.GOMAXPROCS(0), "concurrency")
		f.UintVar(&countWarn, "count-warn", 0, "warning count thareshold")
		f.UintVar(&countCrit, "count-crit", 0, "critical count thareshold")
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string,
	) error {
		return withJobConfig(cmd, checkSnapshots,
			func(m *config.MonitorSnapshots) bool {
				return m.Valid()
			})
	},
}

var countsCmd = &cli.Subcommand{
	Use:   "count",
	Short: "check snapshots count according to rules",

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string,
	) error {
		return withJobConfig(cmd, checkCounts,
			func(m *config.MonitorSnapshots) bool {
				return len(m.Count) > 0
			})
	},
}

var latestCmd = &cli.Subcommand{
	Use:   "latest",
	Short: "check latest snapshots are not too old, according to rules",

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string,
	) error {
		return withJobConfig(cmd, checkLatest,
			func(m *config.MonitorSnapshots) bool {
				return len(m.Latest) > 0
			})
	},
}

var oldestCmd = &cli.Subcommand{
	Use:   "oldest",
	Short: "check oldest snapshots are not too old, according to rules",

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string,
	) error {
		return withJobConfig(cmd, checkOldest,
			func(m *config.MonitorSnapshots) bool {
				return len(m.Oldest) > 0
			})
	},
}

func withStatusClient(cmd *cli.Subcommand, fn func(c *status.Client) error,
) error {
	sockPath := cmd.Config().Global.Control.SockPath
	statusClient, err := status.NewClient("unix", sockPath)
	if err != nil {
		return fmt.Errorf("connect to daemon socket at %q: %w", sockPath, err)
	}
	return fn(statusClient)
}

func withJobConfig(cmd *cli.Subcommand,
	fn func(j *config.JobEnum, resp *monitoringplugin.Response) error,
	filterJob func(m *config.MonitorSnapshots) bool,
) (err error) {
	resp := monitoringplugin.NewResponse("monitor snapshots")

	var foundJob bool
	for j := range jobs(cmd.Config(), snapJob, filterJob) {
		foundJob = true
		if err = fn(j, resp); err != nil {
			err = fmt.Errorf("job %q: %w", j.Name(), err)
			break
		}
	}

	if !foundJob {
		err = fmt.Errorf("job %q: not defined in config", snapJob)
	}
	if err != nil {
		resp.UpdateStatusOnError(err, monitoringplugin.UNKNOWN, "", true)
	}

	resp.OutputAndExit()
	return nil
}

func jobs(c *config.Config, jobName string,
	filterJob func(m *config.MonitorSnapshots) bool,
) iter.Seq[*config.JobEnum] {
	fn := func(yield func(j *config.JobEnum) bool) {
		for i := range c.Jobs {
			j := &c.Jobs[i]
			m := j.MonitorSnapshots()
			ok := (jobName == "" && filterJob(&m)) ||
				(jobName != "" && j.Name() == jobName)
			if ok && !yield(j) {
				break
			}
		}
	}
	return fn
}

func checkSnapshots(j *config.JobEnum, resp *monitoringplugin.Response) error {
	check := snapCheck(resp)
	m := j.MonitorSnapshots()

	if len(m.Count) > 0 {
		if err := check.WithCounts(true).UpdateStatus(j); err != nil {
			return err
		}
	}

	if len(m.Latest) > 0 {
		if err := check.Reset().WithCounts(false).UpdateStatus(j); err != nil {
			return err
		}
	}

	if len(m.Oldest) > 0 {
		return check.Reset().WithOldest(true).UpdateStatus(j)
	}
	return nil
}

func snapCheck(resp *monitoringplugin.Response) *SnapCheck {
	return NewSnapCheck(resp).
		WithMaxProcs(maxProcs).
		WithPrefix(snapPrefix).
		WithThresholds(snapWarn, snapCrit).
		WithCountThresholds(countWarn, countCrit)
}

func checkCounts(j *config.JobEnum, resp *monitoringplugin.Response) error {
	return snapCheck(resp).WithCounts(true).UpdateStatus(j)
}

func checkLatest(j *config.JobEnum, resp *monitoringplugin.Response) error {
	return snapCheck(resp).UpdateStatus(j)
}

func checkOldest(j *config.JobEnum, resp *monitoringplugin.Response) error {
	return snapCheck(resp).WithOldest(true).UpdateStatus(j)
}
