package pruning

import (
	"context"
	"fmt"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
)

type KeepRule interface {
	KeepRule(ctx context.Context, snaps []Snapshot) (destroyList []Snapshot)
}

type Snapshot interface {
	Name() string
	Replicated() bool
	Date() time.Time
}

func RulesFromConfig(in []config.PruningEnum) (rules []KeepRule, err error) {
	rules = make([]KeepRule, len(in))
	for i := range in {
		rules[i], err = RuleFromConfig(in[i])
		if err != nil {
			return nil, fmt.Errorf("cannot build rule #%d: %w", i, err)
		}
	}
	return rules, nil
}

func RuleFromConfig(in config.PruningEnum) (KeepRule, error) {
	switch v := in.Ret.(type) {
	case *config.PruneKeepNotReplicated:
		return NewKeepNotReplicated(), nil
	case *config.PruneKeepLastN:
		return NewKeepLastN(v.Count, v.Regex)
	case *config.PruneKeepRegex:
		return NewKeepRegex(v.Regex, v.Negate)
	case *config.PruneGrid:
		return NewKeepGrid(v)
	default:
		return nil, fmt.Errorf("unknown keep rule type %T", v)
	}
}

// The returned snapshot list is guaranteed to only contains elements of input
// parameter snaps
func PruneSnapshots(ctx context.Context, snaps []Snapshot, keepRules []KeepRule,
) []Snapshot {
	if len(keepRules) == 0 {
		return []Snapshot{}
	}

	remCount := make(map[Snapshot]int, len(snaps))
	for _, r := range keepRules {
		for _, s := range r.KeepRule(ctx, snaps) {
			remCount[s]++
		}
	}

	destroy := make([]Snapshot, 0, len(snaps))
	for _, s := range snaps {
		if cnt, ok := remCount[s]; ok && cnt == len(keepRules) {
			destroy = append(destroy, s)
		}
	}
	return destroy
}
