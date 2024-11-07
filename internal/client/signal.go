package client

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon"
)

var SignalCmd = &cli.Subcommand{
	Use:   "signal {reload | reset JOB | shutdown | stop | wakeup JOB}",
	Short: "send a signal to the daemon",
	Long: `Send a signal to the daemon.

Expected signals:
  reload   Reload TLS certificates
  reset    Abort job's current invocation
  shutdown Stop daemon gracefully
  stop     Stop daemon right now
  wakeup   Wake up job from wait state
`,

	SetupCobra: func(cmd *cobra.Command) {
		cmd.ValidArgs = []string{"reload", "reset", "shutdown", "stop", "wakeup"}
		cmd.Args = cobra.MatchAll(cobra.MinimumNArgs(1),
			func(cmd *cobra.Command, args []string) error {
				switch args[0] {
				case "reload", "shutdown", "stop":
					return cobra.ExactArgs(1)(cmd, args)
				case "reset", "wakeup":
					return cobra.ExactArgs(2)(cmd, args)
				}
				return cobra.OnlyValidArgs(cmd, args)
			})
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand,
		args []string,
	) error {
		return runSignalCmd(subcommand.Config(), args)
	},
}

func runSignalCmd(config *config.Config, args []string) error {
	req := struct {
		Op   string
		Name string
	}{Op: args[0]}

	if len(args) > 1 {
		req.Name = args[1]
	}

	return jsonRequestResponse(config.Global.Control.SockPath,
		daemon.ControlJobEndpointSignal, &req, nil)
}
