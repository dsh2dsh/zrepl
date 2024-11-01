package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dsh2dsh/zrepl/internal/config"
)

func TestCheckClientIdentity(t *testing.T) {
	const keyName = "test"
	const keyToken = "ThBKqH8aZojsKF8FPdKbClQCJPPb2+Abpv1Nl2EQaaU="
	keys := []config.AuthKey{
		{
			Name: keyName,
			Key:  keyToken,
		},
	}
	checker := CheckClientIdentity(keys)

	tests := []struct {
		name       string
		req        func() *http.Request
		statusCode int
	}{
		{
			name: "without Authorization",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/", nil)
			},
			statusCode: http.StatusUnauthorized,
		},
		{
			name: "without bearer",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/", nil)
				r.Header.Set("Authorization", "Basic "+keyToken)
				return r
			},
			statusCode: http.StatusUnauthorized,
		},
		{
			name: "with wrong token",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/", nil)
				r.Header.Set("Authorization", "Bearer foobar")
				return r
			},
			statusCode: http.StatusUnauthorized,
		},
		{
			name: "with client identity",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/", nil)
				r.Header.Set("Authorization", "Bearer "+keyToken)
				return r
			},
			statusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := AppendHandler([]Middleware{checker}, http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					clientIdentity := ClientIdentityFrom(r.Context())
					assert.Equal(t, keyName, clientIdentity)
					w.WriteHeader(http.StatusOK)
				}))
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, tt.req())
			assert.Equal(t, tt.statusCode, resp.Code)
		})
	}
}
