package client

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon"
)

var pprofListenCmd struct {
	daemon.PprofServerControlMsg
}

var PprofListenCmd = &cli.Subcommand{
	Use:   "listen off | [on TCP_LISTEN_ADDRESS]",
	Short: "start a http server exposing go-tool-compatible profiling endpoints at TCP_LISTEN_ADDRESS",
	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
		if len(args) < 1 {
			goto enargs
		}
		switch args[0] {
		case "on":
			pprofListenCmd.Run = true
			if len(args) != 2 {
				return errors.New("must specify TCP_LISTEN_ADDRESS as second positional argument")
			}
			pprofListenCmd.HttpListenAddress = args[1]
		case "off":
			if len(args) != 1 {
				goto enargs
			}
			pprofListenCmd.Run = false
		}

		RunPProf(subcommand.Config())
		return nil
	enargs:
		return errors.New("invalid number of positional arguments")
	},
}

func RunPProf(conf *config.Config) {
	log := log.New(os.Stderr, "", 0)

	die := func() {
		log.Printf("exiting after error")
		os.Exit(1)
	}

	log.Printf("connecting to zrepl daemon")

	err := jsonRequestResponse(conf.Global.Control.SockPath,
		daemon.ControlJobEndpointPProf, pprofListenCmd.PprofServerControlMsg, nil)
	if err != nil {
		log.Printf("error sending control message: %s", err)
		die()
	}
	log.Printf("finished")
}
