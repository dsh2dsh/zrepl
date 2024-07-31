package status

import (
	"github.com/sahilm/fuzzy"

	"github.com/dsh2dsh/zrepl/daemon/snapper"
	"github.com/dsh2dsh/zrepl/replication/report"
)

func filesystemsAsFiltered(items []*report.FilesystemReport,
) ([]filteredFs, int) {
	var maxLen int
	filteredMatches := make([]filteredFs, len(items))
	for i, fs := range items {
		maxLen = max(maxLen, len(fs.Info.Name))
		filteredMatches[i] = filteredFs{Fs: fs}
	}
	return filteredMatches, maxLen
}

type filteredFs struct {
	Fs      *report.FilesystemReport
	Matches []int
}

func filterFilesystems(term string, items []*report.FilesystemReport,
) ([]filteredFs, int) {
	var maxLen int
	filteredMatches := []filteredFs{}
	for _, r := range fuzzy.FindFromNoSort(term, fuzzyFilesystems(items)) {
		fs := items[r.Index]
		maxLen = max(maxLen, len(fs.Info.Name))
		filteredMatches = append(filteredMatches, filteredFs{
			Fs:      fs,
			Matches: r.MatchedIndexes,
		})
	}
	return filteredMatches, maxLen
}

type fuzzyFilesystems []*report.FilesystemReport

func (self fuzzyFilesystems) String(i int) string { return self[i].Info.Name }

func (self fuzzyFilesystems) Len() int { return len(self) }

// --------------------------------------------------

func prunerFsAsFiltered(items []prunerFs) ([]prunerFs, int) {
	var maxLen int
	for i := range items {
		fs := &items[i]
		maxLen = max(maxLen, len(fs.Filesystem))
	}
	return items, maxLen
}

func filterPrunerFs(term string, items []prunerFs) ([]prunerFs, int) {
	var maxLen int
	filteredMatches := []prunerFs{}
	for _, r := range fuzzy.FindFromNoSort(term, fuzzyPrunerFs(items)) {
		item := items[r.Index]
		item.Matches = r.MatchedIndexes
		maxLen = max(maxLen, len(item.Filesystem))
		filteredMatches = append(filteredMatches, item)
	}
	return filteredMatches, maxLen
}

type fuzzyPrunerFs []prunerFs

func (self fuzzyPrunerFs) String(i int) string {
	return self[i].FSReport.Filesystem
}

func (self fuzzyPrunerFs) Len() int { return len(self) }

// --------------------------------------------------

func snapperFsAsFiltered(items []*snapper.ReportFilesystem,
) ([]filteredSnapperFs, int) {
	var maxLen int
	filteredMatches := make([]filteredSnapperFs, len(items))
	for i, fs := range items {
		maxLen = max(maxLen, len(fs.Path))
		filteredMatches[i] = filteredSnapperFs{Fs: fs}
	}
	return filteredMatches, maxLen
}

type filteredSnapperFs struct {
	Fs      *snapper.ReportFilesystem
	Matches []int
}

func filterSnapperFs(term string, items []*snapper.ReportFilesystem,
) ([]filteredSnapperFs, int) {
	var maxLen int
	filteredMatches := []filteredSnapperFs{}
	for _, r := range fuzzy.FindFromNoSort(term, fuzzySnapperFs(items)) {
		fs := items[r.Index]
		maxLen = max(maxLen, len(fs.Path))
		filteredMatches = append(filteredMatches, filteredSnapperFs{
			Fs:      fs,
			Matches: r.MatchedIndexes,
		})
	}
	return filteredMatches, maxLen
}

type fuzzySnapperFs []*snapper.ReportFilesystem

func (self fuzzySnapperFs) String(i int) string { return self[i].Path }

func (self fuzzySnapperFs) Len() int { return len(self) }
