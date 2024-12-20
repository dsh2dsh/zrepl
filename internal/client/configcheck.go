package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

var configcheckArgs struct {
	format string
	what   string
}

var ConfigcheckCmd = &cli.Subcommand{
	Use:   "configcheck",
	Short: "check if config can be parsed without errors",

	ConfigWithIncludes: true,

	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVar(&configcheckArgs.format, "format", "", "dump parsed config object [yaml|json]")
		f.StringVar(&configcheckArgs.what, "what", "all", "what to print [all|config|jobs|logging]")
	},

	Run: func(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
		formatMap := map[string]func(interface{}){
			"": func(i interface{}) {},
			"json": func(i interface{}) {
				if err := json.NewEncoder(os.Stdout).Encode(subcommand.Config()); err != nil {
					panic(err)
				}
			},
			"yaml": func(i interface{}) {
				if err := yaml.NewEncoder(os.Stdout).Encode(subcommand.Config()); err != nil {
					panic(err)
				}
			},
		}

		formatter, ok := formatMap[configcheckArgs.format]
		if !ok {
			return fmt.Errorf("unsupported --format %q", configcheckArgs.format)
		}

		var hadErr bool

		// further: try to build jobs
		confJobs, _, err := job.JobsFromConfig(subcommand.Config())
		if err != nil {
			err := fmt.Errorf("cannot build jobs from config: %w", err)
			if configcheckArgs.what == "jobs" {
				return err
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				confJobs = nil
				hadErr = true
			}
		}

		// further: try to build logging outlets
		outlets, err := logging.OutletsFromConfig(subcommand.Config().Global.Logging)
		if err != nil {
			err := fmt.Errorf("cannot build logging from config: %w", err)
			if configcheckArgs.what == "logging" {
				return err
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				outlets = nil
				hadErr = true
			}
		}

		whatMap := map[string]func(){
			"all": func() {
				o := struct {
					config  *config.Config
					jobs    []job.Job
					logging *logger.Outlets
				}{
					subcommand.Config(),
					confJobs,
					outlets,
				}
				formatter(o)
			},
			"config": func() {
				formatter(subcommand.Config())
			},
			"jobs": func() {
				formatter(confJobs)
			},
			"logging": func() {
				formatter(outlets)
			},
		}

		wf, ok := whatMap[configcheckArgs.what]
		if !ok {
			return fmt.Errorf("unsupported --format %q", configcheckArgs.what)
		}
		wf()

		if hadErr {
			return errors.New("config parsing failed")
		} else {
			return nil
		}
	},
}
