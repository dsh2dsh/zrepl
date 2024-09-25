package monitor

import (
	"context"
	"fmt"
	"iter"
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
	Short: "check snapshots age",

	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{latestCmd, oldestCmd}
	},

	SetupCobra: func(c *cobra.Command) {
		c.Args = cobra.ExactArgs(0)
		f := c.PersistentFlags()
		f.StringVarP(&snapJob, "job", "j", "", "name of the job")
		f.StringVarP(&snapPrefix, "prefix", "p", "", "snapshot prefix")
		f.DurationVarP(&snapCrit, "crit", "c", 0, "critical snapshot age")
		f.DurationVarP(&snapWarn, "warn", "w", 0, "warning snapshot age")
		c.MarkFlagsRequiredTogether("prefix", "crit")
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string,
	) error {
		return withJobConfig(cmd, checkSnapshots)
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
		return withJobConfig(cmd, checkLatest)
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
		return withJobConfig(cmd, checkOldest)
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
) (err error) {
	resp := monitoringplugin.NewResponse("monitor snapshots")

	var foundJob bool
	for j := range jobs(cmd.Config(), snapJob) {
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

func jobs(c *config.Config, jobName string) iter.Seq[*config.JobEnum] {
	fn := func(yield func(j *config.JobEnum) bool) {
		for i := range c.Jobs {
			j := &c.Jobs[i]
			ok := (jobName == "" && j.MonitorSnapshots().Valid()) ||
				(jobName != "" && j.Name() == jobName)
			if ok && !yield(j) {
				break
			}
		}
	}
	return fn
}

func checkSnapshots(j *config.JobEnum, resp *monitoringplugin.Response) error {
	check := NewSnapCheck(resp).
		WithPrefix(snapPrefix).
		WithThresholds(snapWarn, snapCrit)
	if err := check.UpdateStatus(j); err != nil {
		return err
	}
	return check.Reset().WithOldest(true).UpdateStatus(j)
}

func checkLatest(j *config.JobEnum, resp *monitoringplugin.Response) error {
	return NewSnapCheck(resp).
		WithPrefix(snapPrefix).
		WithThresholds(snapWarn, snapCrit).
		UpdateStatus(j)
}

func checkOldest(j *config.JobEnum, resp *monitoringplugin.Response) error {
	return NewSnapCheck(resp).WithOldest(true).
		WithPrefix(snapPrefix).
		WithThresholds(snapWarn, snapCrit).
		UpdateStatus(j)
}
