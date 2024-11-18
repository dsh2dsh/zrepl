package client

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon"
	"github.com/dsh2dsh/zrepl/internal/version"
)

var versionArgs struct {
	Show      string
	Config    *config.Config
	ConfigErr error
}

var VersionCmd = &cli.Subcommand{
	Use:             "version",
	Short:           "print version of zrepl binary and running daemon",
	NoRequireConfig: true,
	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVar(&versionArgs.Show, "show", "", "version info to show (client|daemon)")
	},
	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
		versionArgs.Config = subcommand.Config()
		versionArgs.ConfigErr = subcommand.ConfigParsingError()
		return runVersionCmd()
	},
}

func runVersionCmd() error {
	args := versionArgs
	if args.Show != "daemon" && args.Show != "client" && args.Show != "" {
		return errors.New("show flag must be 'client' or 'server' or be left empty")
	}

	var clientVersion, daemonVersion version.ZreplVersionInformation
	if args.Show == "client" || args.Show == "" {
		clientVersion = version.NewZreplVersionInformation()
		fmt.Printf("client: %s\n", clientVersion.String())
	}

	if args.Show == "daemon" || args.Show == "" {
		if args.ConfigErr != nil {
			return fmt.Errorf("config parsing error: %w", args.ConfigErr)
		}

		err := jsonRequestResponse(args.Config.Global.Control.SockPath,
			daemon.ControlJobEndpointVersion, nil, &daemonVersion)
		if err != nil {
			return fmt.Errorf("server: error: %w\n", err)
		}
		fmt.Printf("server: %s\n", daemonVersion.String())
	}

	if args.Show == "" {
		if clientVersion.Version != daemonVersion.Version {
			fmt.Fprintf(os.Stderr, "WARNING: client version != daemon version, restart zrepl daemon\n")
		}
	}

	return nil
}
