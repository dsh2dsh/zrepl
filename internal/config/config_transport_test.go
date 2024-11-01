package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransportConnect(t *testing.T) {
	tmpl := `
jobs:
- name: foo
  type: push
  connect:
%s
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
`

	mconf := func(s string) string { return fmt.Sprintf(tmpl, s) }

	type test struct {
		Name        string
		ExpectError bool
		Connect     string
	}

	testTable := []test{
		{
			Name:        "http_with_address_and_port",
			ExpectError: false,
			Connect: `
			type: "http"
			server: "http://10.0.0.23:42"
      listener_name: "job"
      client_identity: "client"
			`,
		},
		{
			Name:        "https_with_host_and_port",
			ExpectError: false,
			Connect: `
			type: "http"
			server: "https://server1.foo.bar:8888"
      listener_name: "job"
      client_identity: "client"
			`,
		},
		{
			Name:        "http_without_scheme",
			ExpectError: true,
			Connect: `
			type: "http"
			server: "10.0.0.23"
      listener_name: "job"
      client_identity: "client"
			`,
		},
		{
			Name:        "https_without_address",
			ExpectError: true,
			Connect: `
			type: "http"
			server: "https://"
      listener_name: "job"
      client_identity: "client"
			`,
		},
	}

	for _, tc := range testTable {
		t.Run(tc.Name, func(t *testing.T) {
			require.NotEmpty(t, tc.Connect)
			connect := trimSpaceEachLineAndPad(tc.Connect, "    ")
			conf := mconf(connect)
			config, err := testConfig(t, conf)
			if tc.ExpectError && err == nil {
				t.Errorf("expected test failure, but got valid config %v", config)
				return
			}
			if !tc.ExpectError && err != nil {
				t.Errorf("not expecint test failure but got error: %s", err)
				return
			}
			t.Logf("error=%v", err)
			t.Logf("config=%v", config)
		})
	}
}
