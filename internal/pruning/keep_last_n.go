package pruning

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type KeepLastN struct {
	n  int
	re *regexp.Regexp
}

var _ KeepRule = (*KeepLastN)(nil)

func NewKeepLastN(n int, regex string) (*KeepLastN, error) {
	if n <= 0 {
		return nil, fmt.Errorf(
			"must specify positive number as 'keep last count', got %d", n)
	}

	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("invalid regex %q: %w", regex, err)
	}
	return &KeepLastN{n, re}, nil
}

func (self *KeepLastN) KeepRule(_ context.Context, snaps []Snapshot,
) (destroy []Snapshot) {
	matching, notMatching := partitionSnapList(snaps, func(s Snapshot) bool {
		return self.re.MatchString(s.Name())
	})

	// snaps that don't match the regex are not kept by this rule
	if len(notMatching) != 0 {
		destroy = notMatching
	}

	if self.n > len(matching) {
		return destroy
	}

	slices.SortFunc(matching, self.cmpSnapshots)
	return append(destroy, matching[self.n:]...)
}

func (self *KeepLastN) cmpSnapshots(a, b Snapshot) int {
	if n := b.Date().Compare(a.Date()); n != 0 {
		return n
	}
	return strings.Compare(b.Name(), a.Name())
}
