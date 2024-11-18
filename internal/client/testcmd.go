package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"github.com/dsh2dsh/zrepl/internal/cli"
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

var TestCmd = &cli.Subcommand{
	Use: "test",
	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{testFilter, testPlaceholder, testDecodeResumeToken}
	},
}

var testFilterArgs struct {
	job   string
	all   bool
	input string
}

var testFilter = &cli.Subcommand{
	Use:   "filesystems --job JOB [--all | --input INPUT]",
	Short: "test filesystems filter specified in push or source job",
	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVar(&testFilterArgs.job, "job", "", "the name of the push or source job")
		f.StringVar(&testFilterArgs.input, "input", "", "a filesystem name to test against the job's filters")
		f.BoolVar(&testFilterArgs.all, "all", false, "test all local filesystems")
	},
	Run: runTestFilterCmd,
}

func runTestFilterCmd(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
	if testFilterArgs.job == "" {
		return errors.New("must specify --job flag")
	}
	if !(testFilterArgs.all != (testFilterArgs.input != "")) { // xor
		return errors.New("must set one: --all or --input")
	}

	conf := subcommand.Config()
	job, err := conf.Job(testFilterArgs.job)
	if err != nil {
		return err
	}

	var ff config.FilesystemsFilter
	var df []config.DatasetFilter

	switch j := job.Ret.(type) {
	case *config.SourceJob:
		ff, df = j.Filesystems, j.Datasets
	case *config.PushJob:
		ff, df = j.Filesystems, j.Datasets
	case *config.SnapJob:
		ff, df = j.Filesystems, j.Datasets
	default:
		return fmt.Errorf("job type %T does not have filesystems filter", j)
	}

	f, err := filters.NewFromConfig(ff, df)
	if err != nil {
		return fmt.Errorf("filter invalid: %s", err)
	}

	var fsnames []string
	if testFilterArgs.input != "" {
		fsnames = []string{testFilterArgs.input}
	} else {
		out, err := zfs.ZFSList(ctx, []string{"name"})
		if err != nil {
			return fmt.Errorf("could not list ZFS filesystems: %s", err)
		}
		for _, row := range out {
			fsnames = append(fsnames, row[0])
		}
	}

	fspaths := make([]*zfs.DatasetPath, len(fsnames))
	for i, fsname := range fsnames {
		path, err := zfs.NewDatasetPath(fsname)
		if err != nil {
			return err
		}
		fspaths[i] = path
	}

	hadFilterErr := false
	for _, in := range fspaths {
		var res string
		var errStr string
		pass, err := f.Filter(in)
		switch {
		case err != nil:
			res = "ERROR"
			errStr = err.Error()
			hadFilterErr = true
		case pass:
			res = "ACCEPT"
		default:
			res = "REJECT"
		}
		fmt.Printf("%s\t%s\t%s\n", res, in.ToString(), errStr)
	}

	if hadFilterErr {
		return errors.New("filter errors occurred")
	}
	return nil
}

var testPlaceholderArgs struct {
	ds  string
	all bool
}

var testPlaceholder = &cli.Subcommand{
	Use:   "placeholder [--all | --dataset DATASET]",
	Short: fmt.Sprintf("list received placeholder filesystems (zfs property %q)", zfs.PlaceholderPropertyName),
	Example: `
	placeholder --all
	placeholder --dataset path/to/sink/clientident/fs`,
	NoRequireConfig: true,
	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVar(&testPlaceholderArgs.ds, "dataset", "", "dataset path (not required to exist)")
		f.BoolVar(&testPlaceholderArgs.all, "all", false, "list tab-separated placeholder status of all filesystems")
	},
	Run: runTestPlaceholder,
}

func runTestPlaceholder(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
	var checkDPs []*zfs.DatasetPath
	var datasetWasExplicitArgument bool

	// all actions first
	if testPlaceholderArgs.all {
		datasetWasExplicitArgument = false
		out, err := zfs.ZFSList(ctx, []string{"name"})
		if err != nil {
			return fmt.Errorf("could not list ZFS filesystems: %w", err)
		}
		for _, row := range out {
			dp, err := zfs.NewDatasetPath(row[0])
			if err != nil {
				panic(err)
			}
			checkDPs = append(checkDPs, dp)
		}
	} else {
		datasetWasExplicitArgument = true
		dp, err := zfs.NewDatasetPath(testPlaceholderArgs.ds)
		if err != nil {
			return err
		}
		if dp.Empty() {
			return errors.New("must specify --dataset DATASET or --all")
		}
		checkDPs = append(checkDPs, dp)
	}

	fmt.Printf("IS_PLACEHOLDER\tDATASET\tzrepl:placeholder\n")
	for _, dp := range checkDPs {
		ph, err := zfs.ZFSGetFilesystemPlaceholderState(ctx, dp)
		if err != nil {
			return fmt.Errorf("cannot get placeholder state: %w", err)
		}
		if !ph.FSExists {
			if datasetWasExplicitArgument {
				return fmt.Errorf("filesystem %q does not exist", ph.FS)
			} else {
				// got deleted between ZFSList and ZFSGetFilesystemPlaceholderState
				continue
			}
		}
		is := "yes"
		if !ph.IsPlaceholder {
			is = "no"
		}
		fmt.Printf("%s\t%s\t%s\n", is, dp.ToString(), ph.RawLocalPropertyValue)
	}
	return nil
}

var testDecodeResumeTokenArgs struct {
	token string
}

var testDecodeResumeToken = &cli.Subcommand{
	Use:   "decoderesumetoken --token TOKEN",
	Short: "decode resume token",
	SetupFlags: func(f *pflag.FlagSet) {
		f.StringVar(&testDecodeResumeTokenArgs.token, "token", "", "the resume token obtained from the receive_resume_token property")
	},
	Run: runTestDecodeResumeTokenCmd,
}

func runTestDecodeResumeTokenCmd(ctx context.Context, subcommand *cli.Subcommand, args []string) error {
	if testDecodeResumeTokenArgs.token == "" {
		return errors.New("token argument must be specified")
	}
	token, err := zfs.ParseResumeToken(ctx, testDecodeResumeTokenArgs.token)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	if err := enc.Encode(&token); err != nil {
		panic(err)
	}
	return nil
}
