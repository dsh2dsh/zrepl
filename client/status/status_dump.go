package status

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/client/status/viewmodel"
)

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

func dump(c *Client, job string) error {
	s, err := c.Status()
	if err != nil {
		return err
	}

	if job != "" {
		if _, ok := s.Jobs[job]; !ok {
			return fmt.Errorf("job %q not found", job)
		}
	}

	hline := strings.Repeat("-", 80)

	m := viewmodel.New()
	params := viewmodel.Params{
		Report:                  s.Jobs,
		ReportFetchError:        nil,
		SelectedJob:             nil,
		FSFilter:                func(s string) bool { return true },
		DetailViewWidth:         (1 << 31) - 1,
		ShortKeybindingOverview: "",
	}
	m.Update(params)
	for _, j := range m.Jobs() {
		if job != "" && j.Name() != job {
			continue
		}
		params.SelectedJob = j
		m.Update(params)
		fmt.Println(m.SelectedJob().FullDescription())
		if job != "" {
			return nil
		} else {
			fmt.Println(hline)
		}
	}

	return nil
}
