package status

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
)

var (
	selectedJob     string
	refreshInterval time.Duration
)

var Subcommand = &cli.Subcommand{
	Use:   "status",
	Short: "display daemon status information",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
		addSelectedJob(cmd)
		cmd.Flags().DurationVarP(&refreshInterval, "delay", "d", 1*time.Second,
			"refresh interval")
	},

	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{dumpCmd, rawCmd}
	},

	Run: func(ctx context.Context, cmd *cli.Subcommand, args []string) error {
		return withStatusClient(cmd, func(c *Client) error {
			model := NewStatusTUI(c).WithInitialJob(selectedJob).
				WithUpdateEvery(refreshInterval)
			p := tea.NewProgram(model, tea.WithAltScreen())
			if _, err := p.Run(); err != nil {
				return fmt.Errorf("running program: %w", err)
			}
			return model.Err()
		})
	},
}

var dumpCmd = &cli.Subcommand{
	Use:   "dump",
	Short: "output daemon status information as plain text",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
		addSelectedJob(cmd)
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string,
	) error {
		return withStatusClient(subcommand, func(c *Client) error {
			return dump(c, selectedJob)
		})
	},
}

var rawCmd = &cli.Subcommand{
	Use:   "raw",
	Short: "output daemon status information as JSON",

	SetupCobra: func(cmd *cobra.Command) {
		cmd.Args = cobra.ExactArgs(0)
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string,
	) error {
		return withStatusClient(subcommand, func(c *Client) error {
			return raw(c)
		})
	},
}

func addSelectedJob(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&selectedJob, "job", "j", "",
		"only show specified job")
}

func withStatusClient(subcommand *cli.Subcommand, fn func(c *Client) error,
) error {
	sockPath := subcommand.Config().Global.Control.SockPath
	statusClient, err := NewClient("unix", sockPath)
	if err != nil {
		return fmt.Errorf("connect to daemon socket at %q: %w", sockPath, err)
	}
	return fn(statusClient)
}

func dump(client *Client, jobName string) error {
	status, err := client.Status()
	if err != nil {
		return err
	}

	jobRender := NewJobRender()
	if jobName != "" {
		return dumpJob(jobRender, jobName, status.Jobs)
	}

	jobs := make([]string, 0, len(status.Jobs))
	for name, j := range status.Jobs {
		if !j.Internal() && j.JobSpecific != nil {
			jobs = append(jobs, name)
		}
	}
	slices.Sort(jobs)

	for i, name := range jobs {
		if err := dumpJob(jobRender, name, status.Jobs); err != nil {
			return err
		}
		if i < len(jobs)-1 {
			fmt.Printf("\n\n---\n\n")
		}
	}

	return nil
}

func dumpJob(render *JobRender, name string, jobs map[string]*job.Status) error {
	j, ok := jobs[name]
	if !ok {
		return fmt.Errorf("job %q doesn't exists", name)
	}
	render.SetJob(j)
	fmt.Println("Job:", name)
	fmt.Print(strings.TrimRight(render.View(), "\n"))
	return nil
}

func raw(c *Client) error {
	b, err := c.StatusRaw()
	if err != nil {
		return err
	}
	if _, err := io.Copy(os.Stdout, bytes.NewReader(b)); err != nil {
		return err
	}
	return nil
}
