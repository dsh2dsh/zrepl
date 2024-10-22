package hooks

import (
	"context"
	"fmt"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type HookJobCallback func(ctx context.Context) error

type CallbackHook struct {
	cb            HookJobCallback
	filter        zfs.DatasetFilter
	displayString string
}

func NewCallbackHookForFilesystem(displayString string, fs *zfs.DatasetPath,
	cb HookJobCallback,
) *CallbackHook {
	filter, _ := filters.NewFromConfig(nil, []config.DatasetFilter{
		{Pattern: fs.ToString()},
	})
	return NewCallbackHook(displayString, cb, filter)
}

func NewCallbackHook(displayString string, cb HookJobCallback,
	filter zfs.DatasetFilter,
) *CallbackHook {
	return &CallbackHook{
		cb:            cb,
		filter:        filter,
		displayString: displayString,
	}
}

func (h *CallbackHook) Filesystems() zfs.DatasetFilter { return h.filter }

func (h *CallbackHook) ErrIsFatal() bool {
	return false // callback is by definition
}

func (h *CallbackHook) String() string { return h.displayString }

type CallbackHookReport struct {
	Name string
	Err  error
}

func (r *CallbackHookReport) String() string {
	if r.HadError() {
		return r.Error()
	}
	return r.Name
}

func (r *CallbackHookReport) HadError() bool { return r.Err != nil }

func (r *CallbackHookReport) Error() string {
	return fmt.Sprintf("%s error: %s", r.Name, r.Err)
}

func (h *CallbackHook) Run(ctx context.Context, edge Edge, phase Phase,
	dryRun bool, extra Env, state map[any]any,
) HookReport {
	return &CallbackHookReport{Name: h.displayString, Err: h.cb(ctx)}
}
