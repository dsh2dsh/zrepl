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
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
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
		f.StringVar(&configcheckArgs.format, "format", "",
			"dump parsed config object [yaml|json]")
		f.StringVar(&configcheckArgs.what, "what", "all",
			"what to print [all|config|jobs|logging]")
	},

	Run: func(_ context.Context, subcommand *cli.Subcommand, _ []string) error {
		return checkConfig(subcommand)
	},
}

func checkConfig(subcommand *cli.Subcommand) error {
	c := subcommand.Config()
	var hadErr bool

	// further: try to build logging outlets
	_, err := logging.OutletsFromConfig(c.Global.Logging)
	if err != nil {
		err := fmt.Errorf("cannot build logging from config: %w", err)
		if configcheckArgs.what == "logging" {
			return err
		} else {
			fmt.Fprintln(os.Stderr, err)
			hadErr = true
		}
	}

	// further: try to build jobs
	_, _, err = job.JobsFromConfig(c)
	if err != nil {
		err := fmt.Errorf("cannot build jobs from config: %w", err)
		if configcheckArgs.what == "jobs" {
			return err
		} else {
			fmt.Fprintln(os.Stderr, err)
			hadErr = true
		}
	}

	switch configcheckArgs.format {
	case "":
	case "json":
		if err := json.NewEncoder(os.Stdout).Encode(c); err != nil {
			return fmt.Errorf("failed encode to json: %w", err)
		}
	case "yaml":
		if err := yaml.NewEncoder(os.Stdout).Encode(c); err != nil {
			return fmt.Errorf("failed encode to yaml: %w", err)
		}
	default:
		return fmt.Errorf("unsupported --format %q", configcheckArgs.format)
	}

	if hadErr {
		return errors.New("config parsing failed")
	}
	return nil
}
