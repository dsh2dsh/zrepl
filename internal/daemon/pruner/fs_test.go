package pruner

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

func Test_snapshotRanges(t *testing.T) {
	snapshotNames := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	snapshots := make([]pruning.Snapshot, len(snapshotNames))
	for i, name := range snapshotNames {
		snapshots[i] = &snapshot{
			fsv: &pdu.FilesystemVersion{
				Type: pdu.FilesystemVersion_Snapshot,
				Name: name,
			},
		}
	}

	tests := []struct {
		name    string
		destroy []string
		want    []string
	}{
		{
			name: "empty",
			want: []string{},
		},
		{
			name:    "one",
			destroy: []string{"3"},
			want:    []string{"3"},
		},
		{
			name:    "all",
			destroy: snapshotNames,
			want:    []string{"1%10"},
		},
		{
			name:    "interleaved",
			destroy: []string{"1", "3", "9"},
			want:    []string{"1", "3", "9"},
		},
		{
			name:    "some range",
			destroy: []string{"6", "7", "8", "9"},
			want:    []string{"6%9"},
		},
		{
			name:    "some range and some interleaved",
			destroy: []string{"1", "6", "7", "8", "10"},
			want:    []string{"1", "6%8", "10"},
		},
		{
			name:    "2 ranges",
			destroy: []string{"1", "2", "3", "6", "7", "8", "9", "10"},
			want:    []string{"1%3", "6%10"},
		},
		{
			name:    "2 ranges and interleaved",
			destroy: []string{"1", "2", "3", "5", "7", "8", "9", "10"},
			want:    []string{"1%3", "5", "7%10"},
		},
		{
			name:    "one step ranges",
			destroy: []string{"1", "2", "4", "5", "8", "9", "10"},
			want:    []string{"1", "2", "4", "5", "8%10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destroy := make([]pruning.Snapshot, len(tt.destroy))
			for i, name := range tt.destroy {
				destroy[i] = &snapshot{
					fsv: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: name,
					},
				}
			}
			names := snapshotRanges(snapshots, destroy)
			assert.Equal(t, tt.want, names)
		})
	}
}
