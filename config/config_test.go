package config

import (
	"bufio"
	"bytes"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"text/template"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSampleConfigsAreParsedWithoutErrors(t *testing.T) {
	paths, err := filepath.Glob("./samples/*")
	if err != nil {
		t.Errorf("glob failed: %+v", err)
	}

	paths = append(paths, "../packaging/systemd-default-zrepl.yml")

	for _, p := range paths {

		if path.Ext(p) != ".yml" {
			t.Logf("skipping file %s", p)
			continue
		}

		t.Run(p, func(t *testing.T) {
			c, err := ParseConfig(p)
			if err != nil {
				t.Errorf("error parsing %s:\n%+v", p, err)
			}

			t.Logf("file: %s", p)
			t.Log(pretty.Sprint(c))
		})

	}
}

// template must be a template/text template with a single '{{ . }}' as placeholder for val
//
//nolint:deadcode,unused
func testValidConfigTemplate(t *testing.T, tmpl string, val string) *Config {
	tmp, err := template.New("master").Parse(tmpl)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	err = tmp.Execute(&buf, val)
	if err != nil {
		panic(err)
	}
	return testValidConfig(t, buf.String())
}

func testValidConfig(t *testing.T, input string) *Config {
	t.Helper()
	conf, err := testConfig(t, input)
	require.NoError(t, err)
	require.NotNil(t, conf)
	return conf
}

func testConfig(t *testing.T, input string) (*Config, error) {
	t.Helper()
	return ParseConfigBytes([]byte(input))
}

func trimSpaceEachLineAndPad(s, pad string) string {
	var out strings.Builder
	scan := bufio.NewScanner(strings.NewReader(s))
	for scan.Scan() {
		fmt.Fprintf(&out, "%s%s\n", pad, bytes.TrimSpace(scan.Bytes()))
	}
	return out.String()
}

func TestTrimSpaceEachLineAndPad(t *testing.T) {
	foo := `
	foo
	bar baz
	`
	assert.Equal(t, "  \n  foo\n  bar baz\n  \n", trimSpaceEachLineAndPad(foo, "  "))
}

func TestEmptyConfig(t *testing.T) {
	cases := []string{
		"",
		"\n",
		"---",
		"---\n",
	}
	for _, input := range cases {
		config := testValidConfig(t, input)
		require.NotNil(t, config)
		require.NotNil(t, config.Global)
		require.Empty(t, config.Jobs)
	}
}

func TestPushJob(t *testing.T) {
	c := testValidConfig(t, `
jobs:
  - name: "foo"
    type: "push"
    connect:
      type: "local"
      listener_name: "foo"
      client_identity: "bar"
    filesystems:
      "<": true
    snapshotting:
      type: "manual"
    pruning:
      keep_sender:
        - type: "not_replicated"
`)

	require.NotEmpty(t, c.Jobs)
	pushJob := c.Jobs[0].Ret.(*PushJob)
	require.NotNil(t, pushJob)
	assert.True(t, pushJob.Replication.OneStep)
}

func TestPushJob_withOneStep(t *testing.T) {
	c := testValidConfig(t, `
jobs:
  - name: "foo"
    type: "push"
    connect:
      type: "local"
      listener_name: "foo"
      client_identity: "bar"
    filesystems:
      "<": true
    snapshotting:
      type: "manual"
    replication:
      one_step: false
    pruning:
      keep_sender:
        - type: "not_replicated"
`)

	require.NotEmpty(t, c.Jobs)
	pushJob := c.Jobs[0].Ret.(*PushJob)
	require.NotNil(t, pushJob)
	assert.False(t, pushJob.Replication.OneStep)
}

func TestPullJob(t *testing.T) {
	c := testValidConfig(t, `
jobs:
  - name: "foo"
    type: "pull"
    connect:
      type: "tls"
      address: "server1.foo.bar:8888"
      ca: "/certs/ca.crt"
      cert: "/certs/cert.crt"
      key: "/certs/key.pem"
      server_cn: "server1"
    root_fs: "pool2/backup_servers"
    pruning:
      keep_sender:
        - type: "not_replicated"
`)

	require.NotEmpty(t, c.Jobs)
	pullJob := c.Jobs[0].Ret.(*PullJob)
	require.NotNil(t, pullJob)
	assert.True(t, pullJob.Replication.OneStep)
}

func TestPullJob_withOneStep(t *testing.T) {
	c := testValidConfig(t, `
jobs:
  - name: "foo"
    type: "pull"
    connect:
      type: "tls"
      address: "server1.foo.bar:8888"
      ca: "/certs/ca.crt"
      cert: "/certs/cert.crt"
      key: "/certs/key.pem"
      server_cn: "server1"
    root_fs: "pool2/backup_servers"
    replication:
      one_step: false
    pruning:
      keep_sender:
        - type: "not_replicated"
`)

	require.NotEmpty(t, c.Jobs)
	pullJob := c.Jobs[0].Ret.(*PullJob)
	require.NotNil(t, pullJob)
	assert.False(t, pullJob.Replication.OneStep)
}
