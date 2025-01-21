package hooks

import (
	"fmt"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func ListFromConfig(in []config.HookCommand) (List, error) {
	hl := make(List, len(in))
	for i, h := range in {
		h, err := NewCommandHook(&h)
		if err != nil {
			return nil, fmt.Errorf("create hook #%d: %w", i+1, err)
		}
		hl[i] = h
	}
	return hl, nil
}

type List []*CommandHook

func (self List) Slice() []*CommandHook { return []*CommandHook(self) }

func (self List) WithCombinedOutput() List {
	for _, h := range self {
		h.WithCombinedOutput()
	}
	return self
}

func (self List) CopyFilteredForFilesystem(fs *zfs.DatasetPath) (List, error) {
	ret := make(List, 0, len(self))
	for _, h := range self {
		if h.Filesystems().Empty() {
			ret = append(ret, h)
			continue
		}
		if ok, err := h.Filesystems().Filter(fs); err != nil {
			return nil, err
		} else if ok {
			ret = append(ret, h)
		}
	}
	return ret, nil
}
