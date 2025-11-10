package job

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/config"
)

func TestValidateReceivingSidesDoNotOverlap(t *testing.T) {
	type testCase struct {
		err   bool
		input []string
	}
	tcs := []testCase{
		{false, nil},
		{false, []string{}},
		{false, []string{""}}, // not our job to determine valid paths
		{false, []string{"a"}},
		{false, []string{"some/path"}},
		{false, []string{"zroot/sink1", "zroot/sink2", "zroot/sink3"}},
		{false, []string{"zroot/foo", "zroot/foobar"}},
		{true, []string{"zroot/b", "zroot/b"}},
		{true, []string{"zroot/foo", "zroot/foo/bar", "zroot/baz"}},
		{false, []string{"a/x", "b/x"}},
		{false, []string{"a", "b"}},
		{true, []string{"a", "a"}},
		{true, []string{"a/x/y", "a/x"}},
		{true, []string{"a/x", "a/x/y"}},
		{true, []string{"a/x", "b/x", "a/x/y"}},
		{true, []string{"a", "a/b", "a/c", "a/b"}},
		{true, []string{"a/b", "a/c", "a/b", "a/d", "a/c"}},
	}

	for _, tc := range tcs {
		t.Logf("input: %v", tc.input)
		err := validateReceivingSidesDoNotOverlap(tc.input)
		if tc.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func validateReceivingSidesDoNotOverlap(receivingRootFSs []string) error {
	if len(receivingRootFSs) == 0 {
		return nil
	}
	rfss := make([]string, len(receivingRootFSs))
	copy(rfss, receivingRootFSs)
	sort.Slice(rfss, func(i, j int) bool {
		return strings.Compare(rfss[i], rfss[j]) == -1
	})
	// add tailing slash because of hierarchy-simulation
	// rootfs/ is not root of rootfs2/
	for i := range rfss {
		rfss[i] += "/"
	}
	// idea:
	//   no path in rfss must be prefix of another
	//
	// rfss is now lexicographically sorted, which means that
	// if i is prefix of j, i < j (in lexicographical order)
	// thus,
	// if any i is prefix of i+n (n >= 1), there is overlap
	for i := 0; i < len(rfss)-1; i++ {
		if strings.HasPrefix(rfss[i+1], rfss[i]) {
			return errors.New("receiving jobs with overlapping root filesystems are forbidden")
		}
	}
	return nil
}

func TestJobIDErrorHandling(t *testing.T) {
	tmpl := `
jobs:
- name: %s
  type: push
  connect:
    type: local
    listener_name: "zdisk"
    client_identity: bar
  filesystems: {"<": true}
  snapshotting:
    type: manual
  pruning:
    keep_sender:
    - type: last_n
      count: 10
    keep_receiver:
    - type: last_n
      count: 10

- name: "zdisk"
  type: "sink"
  root_fs: "zdisk/zrepl"
  serve:
    type: "local"
    listener_name: "zdisk"
`
	fill := func(s string) string { return fmt.Sprintf(tmpl, s) }

	type Case struct {
		jobName string
		valid   bool
	}
	cases := []Case{
		{"validjobname", true},
		{"valid with spaces", true},
		{"invalid\twith\ttabs", false},
		{"invalid#withdelimiter", false},
		{"invalid@withdelimiter", false},
		{"withnewline\\nmiddle", false},
		{"withnewline\\n", false},
		{"withslash/", false},
		{"withslash/inthemiddle", false},
		{"/", false},
	}

	for i := range cases {
		t.Run(cases[i].jobName, func(t *testing.T) {
			c := cases[i]

			conf, err := config.ParseConfigBytes("", []byte(fill(c.jobName)))
			require.NoError(t, err, "not expecting yaml-config to know about job ids")
			require.NotNil(t, conf)
			jobs, _, err := JobsFromConfig(conf)

			if c.valid {
				require.NoError(t, err)
				require.Len(t, jobs, 2)
				assert.Equal(t, c.jobName, jobs[0].Name())
			} else {
				t.Logf("error: %s", err)
				require.Error(t, err)
				assert.Nil(t, jobs)
			}
		})
	}
}

func TestSampleConfigsAreBuiltWithoutErrors(t *testing.T) {
	paths, err := filepath.Glob("../../../internal/config/samples/*")
	if err != nil {
		t.Errorf("glob failed: %+v", err)
	}

	type additionalCheck struct {
		state int
		test  func(t *testing.T, jobs []Job)
	}
	additionalChecks := map[string]*additionalCheck{}

	for _, p := range paths {

		if path.Ext(p) != ".yml" {
			t.Logf("skipping file %s", p)
			continue
		}

		filename := path.Base(p)
		t.Logf("checking for presence additional checks for file %q", filename)
		additionalCheck := additionalChecks[filename]
		if additionalCheck == nil {
			t.Logf("no additional checks")
		} else {
			t.Logf("additional check present")
			additionalCheck.state = 1
		}

		t.Run(p, func(t *testing.T) {
			c, err := config.ParseConfig(p)
			if err != nil {
				t.Fatalf("error parsing %s:\n%+v", p, err)
			}

			t.Logf("file: %s", p)
			t.Logf("%#v", c)

			jobs, _, err := JobsFromConfig(c)
			t.Logf("jobs: %#v", jobs)
			require.NoError(t, err)

			if additionalCheck != nil {
				additionalCheck.test(t, jobs)
				additionalCheck.state = 2
			}
		})

	}

	for basename, c := range additionalChecks {
		if c.state == 0 {
			panic("univisited additional check " + basename)
		}
	}
}

func TestReplicationOptions(t *testing.T) {
	tmpl := `
jobs:
- name: foo
  type: push
  connect:
    type: local
    listener_name: "zdisk"
    client_identity: bar
  filesystems: {"<": true}
  %s
  snapshotting:
    type: manual
  pruning:
    keep_sender:
    - type: last_n
      count: 10
    keep_receiver:
    - type: last_n
      count: 10

- name: "zdisk"
  type: "sink"
  root_fs: "zdisk/zrepl"
  serve:
    type: "local"
    listener_name: "zdisk"
`

	type Test struct {
		name        string
		input       string
		expectOk    func(t *testing.T, a *ActiveSide, m *modePush)
		expectError bool
	}

	tests := []Test{
		{
			name: "defaults",
			input: `
  replication: {}
`,
			expectOk: func(t *testing.T, a *ActiveSide, m *modePush) {},
		},
		{
			name: "steps_zero",
			input: `
  replication:
    concurrency:
      steps: 0
`,
			expectError: true,
		},
		{
			name: "size_estimates_zero",
			input: `
  replication:
    concurrency:
      size_estimates: 0
`,
			expectOk: func(t *testing.T, a *ActiveSide, m *modePush) {
				assert.Zero(t, m.drySendConcurrency)
			},
		},
		{
			name: "custom_values",
			input: `
  replication:
    concurrency:
      steps: 23
      size_estimates: 42
`,
			expectOk: func(t *testing.T, a *ActiveSide, m *modePush) {
				assert.Equal(t, 23, a.replicationDriverConfig.StepQueueConcurrency)
				assert.Equal(t, 42, m.drySendConcurrency)
			},
		},
		{
			name: "negative_values_forbidden",
			input: `
  replication:
    concurrency:
      steps: -23
      size_estimates: -42
`,
			expectError: true,
		},
	}

	fill := func(s string) string { return fmt.Sprintf(tmpl, s) }

	for _, ts := range tests {
		t.Run(ts.name, func(t *testing.T) {
			assert.NotEqual(t, (ts.expectError), (ts.expectOk != nil))

			cstr := fill(ts.input)
			t.Logf("testing config:\n%s", cstr)
			c, err := config.ParseConfigBytes("", []byte(cstr))
			if ts.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			jobs, _, err := JobsFromConfig(c)
			switch {
			case ts.expectOk != nil:
				require.NoError(t, err)
				require.NotNil(t, c)
				require.NoError(t, err)
				require.Len(t, jobs, 2)
				a := jobs[0].(*ActiveSide)
				m := a.mode.(*modePush)
				ts.expectOk(t, a, m)
			case ts.expectError:
				require.Error(t, err)
			default:
				t.Fatalf("test must define expectOk or expectError")
			}
		})
	}
}
