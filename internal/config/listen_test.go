package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListen_Validator(t *testing.T) {
	tests := []struct {
		name    string
		listen  Listen
		invalid bool
	}{
		{
			name:    "empty",
			invalid: true,
		},
		{
			name:   "with addr and metrics",
			listen: Listen{Addr: "127.0.0.1:80", Metrics: true},
		},
		{
			name:   "with unix and control",
			listen: Listen{Unix: "/notexists", Control: true},
		},
		{
			name:    "without control and metrics",
			listen:  Listen{Addr: "127.0.0.1:80"},
			invalid: true,
		},
		{
			name: "with addr and unix",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				Unix:    "/notexists",
				Metrics: true,
			},
		},
		{
			name: "with tls_key without tls_cert",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				TLSKey:  "/notexists",
				Metrics: true,
			},
			invalid: true,
		},
		{
			name: "with tls_cert",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				TLSCert: "/notexists",
				Metrics: true,
			},
		},
		{
			name: "with tls_key",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				TLSCert: "/notexists",
				TLSKey:  "/notexists",
				Metrics: true,
			},
		},
		{
			name:    "with wrong addr",
			listen:  Listen{Addr: "127.0.0.1", Metrics: true},
			invalid: true,
		},
		{
			name:    "with wrong unix",
			listen:  Listen{Unix: "/", Control: true},
			invalid: true,
		},
		{
			name: "with wrong tls_cert",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				TLSCert: "/",
				Metrics: true,
			},
			invalid: true,
		},
		{
			name: "with wrong tls_key",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				TLSCert: "/notexists",
				TLSKey:  "/",
				Metrics: true,
			},
			invalid: true,
		},
		{
			name: "with control and metrics",
			listen: Listen{
				Addr:    "127.0.0.1:80",
				Control: true,
				Metrics: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validator().Struct(&tt.listen)
			if tt.invalid {
				t.Log(err)
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
