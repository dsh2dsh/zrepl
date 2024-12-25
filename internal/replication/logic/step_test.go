package logic

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

func Test_makeSteps(t *testing.T) {
	resume := &Step{
		to: &pdu.FilesystemVersion{
			Type: pdu.FilesystemVersion_Snapshot,
			Name: "zrepl_full",
		},
	}

	tests := []struct {
		name   string
		prefix string
		resume *Step
		snaps  []*pdu.FilesystemVersion
		want   []*Step
	}{
		{
			name: "without snapshots",
			want: []*Step{},
		},
		{
			name: "initial with 1 snapshot",
			snaps: []*pdu.FilesystemVersion{
				nil,
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
			},
			want: []*Step{
				{
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
				},
			},
		},
		{
			name: "initial with 2 snapshots",
			snaps: []*pdu.FilesystemVersion{
				nil,
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
			},
			want: []*Step{
				{
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name: "initial with 3 snapshots",
			snaps: []*pdu.FilesystemVersion{
				nil,
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				{
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					multi: true,
				},
			},
		},
		{
			name:   "resume without snapshots",
			resume: resume,
			want:   []*Step{resume},
		},
		{
			name:   "resume with 1 snapshot",
			resume: resume,
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
			},
			want: []*Step{resume},
		},
		{
			name:   "resume with 2 snapshots",
			resume: resume,
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
			},
			want: []*Step{
				resume,
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name:   "resume with 3 snapshots",
			resume: resume,
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				resume,
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					multi: true,
				},
			},
		},
		{
			name: "bookmark with 2 snapshots",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
			},
		},
		{
			name: "bookmark with 3 snapshots",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
				},
			},
		},
		{
			name: "bookmark with 4 snapshots",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_4"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_4",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with 2 snapshots",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name:   "prefix with 3 snapshots",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with alien",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "full"},
			},
			want: []*Step{},
		},
		{
			name:   "prefix with 2 snapshots and alien",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name:   "prefix with 2 snapshots and 2 aliens",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name:   "prefix with 2 snapshots and alien between",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
			},
		},
		{
			name:   "prefix with 3 snapshots and aliens between",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
			},
		},
		{
			name:   "prefix with 4 snapshots and alien between",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with 5 snapshots and 2 aliens between",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with 5 snapshots and 2 aliens between 2",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
				},
			},
		},
		{
			name:   "prefix with 5 snapshots and 2 aliens between 3",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with 5 snapshots and 2 aliens",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with alien",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_full"},
			},
			want: []*Step{},
		},
		{
			name:   "prefix with initial, 2 aliens and snapshot",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				nil,
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
			},
			want: []*Step{
				{
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
				},
			},
		},
		{
			name:   "prefix with resume, 3 snapshots and alien",
			prefix: "zrepl_",
			resume: resume,
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
			},
			want: []*Step{
				resume,
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					multi: true,
				},
			},
		},
		{
			name:   "prefix with resume, snapshot, alien and 2 snapshots",
			prefix: "zrepl_",
			resume: resume,
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_full"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				resume,
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_full",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
			},
		},
		{
			name:   "prefix with bookmark, snapshot and alien",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
			},
		},
		{
			name:   "prefix with bookmark, alien and snapshot",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
			},
		},
		{
			name:   "prefix with bookmark, alien and 2 snapshots",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_3",
					},
				},
			},
		},
		{
			name:   "prefix with bookmark, alien and 3 snapshots",
			prefix: "zrepl_",
			snaps: []*pdu.FilesystemVersion{
				{Type: pdu.FilesystemVersion_Bookmark, Name: "zrepl_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "alien_1"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_2"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_3"},
				{Type: pdu.FilesystemVersion_Snapshot, Name: "zrepl_4"},
			},
			want: []*Step{
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Bookmark,
						Name: "zrepl_1",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
				},
				{
					from: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_2",
					},
					to: &pdu.FilesystemVersion{
						Type: pdu.FilesystemVersion_Snapshot,
						Name: "zrepl_4",
					},
					multi: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeSteps(nil, tt.prefix, tt.resume, tt.snaps)
			assert.Equal(t, tt.want, got)
		})
	}
}
