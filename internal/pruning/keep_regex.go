package pruning

import (
	"context"
	"fmt"
	"regexp"
)

type KeepRegex struct {
	expr   *regexp.Regexp
	negate bool
}

var _ KeepRule = (*KeepRegex)(nil)

func NewKeepRegex(expr string, negate bool) (*KeepRegex, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("re compile %q: %w", expr, err)
	}
	return &KeepRegex{re, negate}, nil
}

func MustKeepRegex(expr string, negate bool) *KeepRegex {
	k, err := NewKeepRegex(expr, negate)
	if err != nil {
		panic(err)
	}
	return k
}

func (k *KeepRegex) KeepRule(_ context.Context, snaps []Snapshot) []Snapshot {
	return filterSnapList(snaps, func(s Snapshot) bool {
		if k.negate {
			return k.expr.FindStringIndex(s.Name()) != nil
		} else {
			return k.expr.FindStringIndex(s.Name()) == nil
		}
	})
}
