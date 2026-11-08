package filters

import "github.com/dsh2dsh/zrepl/internal/config"

func applyAutoFixes(item *config.DatasetFilter, prev config.DatasetFilter) {
	if selfExcluding(item, prev) {
		item.Recursive = new(false)
	}
}

func selfExcluding(item *config.DatasetFilter, prev config.DatasetFilter) bool {
	return item.Pattern == prev.Pattern &&
		!prev.Exclude && item.Exclude &&
		prev.Recursive != nil && *prev.Recursive && item.Recursive == nil &&
		!prev.Shell && !item.Shell
}
