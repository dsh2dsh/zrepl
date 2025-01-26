package snapper

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/daemon/hooks"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type planArgs struct {
	prefix          string
	timestampFormat string
	timestampLocal  bool
	hooks           hooks.List
	concurrency     int
}

type plan struct {
	args  planArgs
	snaps map[*zfs.DatasetPath]*progress

	hookMatchCount map[hooks.Hook]int
}

func makePlan(args planArgs, fss []*zfs.DatasetPath) *plan {
	p := &plan{args: args}
	return p.init(fss)
}

type SnapState uint

const (
	SnapPending SnapState = 1 << iota
	SnapStarted
	SnapDone
	SnapError
)

func (self SnapState) String() string {
	switch self {
	case SnapPending:
		return "SnapPending"
	case SnapStarted:
		return "SnapStarted"
	case SnapDone:
		return "SnapDone"
	case SnapError:
		return "SnapError"
	}
	return "SnapState(" + strconv.FormatInt(int64(self), 10) + ")"
}

func (self *plan) init(paths []*zfs.DatasetPath) *plan {
	self.makeHooksCount()
	self.makeSnapsProgress(paths)
	return self
}

func (self *plan) makeHooksCount() {
	self.hookMatchCount = make(map[hooks.Hook]int, len(self.args.hooks))
	for _, h := range self.args.hooks {
		self.hookMatchCount[h] = 0
	}
}

func (self *plan) makeSnapsProgress(paths []*zfs.DatasetPath) {
	for i, p := range paths {
		if p.RecursiveParent() != nil {
			paths[i] = p.RecursiveParent()
		}
	}
	paths = slices.Compact(paths)

	self.snaps = make(map[*zfs.DatasetPath]*progress, len(paths))
	for _, fs := range paths {
		self.snaps[fs] = NewProgress()
	}
}

func (self *plan) snapName() string {
	return fmt.Sprintf("%s%s", self.args.prefix,
		self.formatNow(self.args.timestampFormat, self.args.timestampLocal))
}

func (self *plan) formatNow(format string, localTime bool) string {
	now := time.Now()
	if !localTime {
		now = now.UTC()
	}

	switch strings.ToLower(format) {
	case "dense":
		format = "20060102_150405_MST"
	case "human":
		format = "2006-01-02_15:04:05"
	case "iso-8601":
		format = "2006-01-02T15:04:05.000Z"
	case "unix-seconds":
		return strconv.FormatInt(now.Unix(), 10)
	}
	return now.Format(format)
}

func (self *plan) execute(ctx context.Context, dryRun bool) bool {
	var anyFsHadErr bool
	var g errgroup.Group
	g.SetLimit(self.args.concurrency)

	// TODO channel programs -> allow a little jitter?
	for fs, progress := range self.snaps {
		snapName := self.snapName()
		ctx := logging.With(ctx, slog.String("fs", fs.ToString()),
			slog.Bool("recursive", fs.Recursive()),
			slog.String("snap", snapName))

		hookPlan := self.hookPlan(ctx, fs, snapName)
		if hookPlan == nil {
			anyFsHadErr = true
			progress.StateError()
			continue
		}

		g.Go(func() error {
			return progress.CreateSnapshot(ctx, dryRun, snapName, hookPlan)
		})
	}

	if err := g.Wait(); err != nil {
		return false
	}
	self.logUnmatchedHooks(ctx)
	return !anyFsHadErr
}

func (self *plan) hookPlan(ctx context.Context, fs *zfs.DatasetPath,
	snapName string,
) *hooks.Plan {
	filteredHooks, err := self.args.hooks.CopyFilteredForFilesystem(fs)
	if err != nil {
		logger.WithError(getLogger(ctx), err, "unexpected filter error")
		return nil
	}
	// account for running hooks
	self.countHooks(filteredHooks)

	jobCallback := hooks.NewCallbackHookForFilesystem("snapshot", fs,
		func(ctx context.Context) error {
			return createSnapshot(ctx, fs, snapName)
		})

	hookPlan, err := hooks.NewPlan(filteredHooks, hooks.PhaseSnapshot,
		jobCallback, map[string]string{
			hooks.EnvFS:       fs.ToString(),
			hooks.EnvSnapshot: snapName,
		})
	if err != nil {
		logger.WithError(getLogger(ctx), err, "cannot create job hook plan")
		return nil
	}
	return hookPlan
}

func createSnapshot(ctx context.Context, fs *zfs.DatasetPath, snapName string,
) error {
	l := getLogger(ctx)
	l.Debug("create snapshot")
	err := zfs.ZFSSnapshot(ctx, fs, snapName, fs.Recursive())
	if err != nil {
		logger.WithError(l, err, "cannot create snapshot")
		return err
	}
	return nil
}

func (self *plan) countHooks(filteredHooks hooks.List) {
	for _, h := range filteredHooks {
		self.hookMatchCount[h]++
	}
}

func (self *plan) logUnmatchedHooks(ctx context.Context) {
	argsHooks := self.args.hooks.Slice()
	l := getLogger(ctx)
	for h, cnt := range self.hookMatchCount {
		if cnt != 0 {
			continue
		}
		hookIdx := slices.IndexFunc(argsHooks,
			func(h2 *hooks.CommandHook) bool { return h2 == h })
		l.With(slog.String("hook", h.String()),
			slog.Int("hook_number", hookIdx+1)).
			Warn("hook did not match any snapshotted filesystems")
	}
}

func (self *plan) report() []*ReportFilesystem {
	reports := make([]*ReportFilesystem, 0, len(self.snaps))
	for fs, p := range self.snaps {
		reports = append(reports, p.Report(fs.ToString()))
	}
	slices.SortFunc(reports, func(a, b *ReportFilesystem) int {
		return strings.Compare(a.Path, b.Path)
	})
	return reports
}
