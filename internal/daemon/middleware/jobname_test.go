package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractJobName(t *testing.T) {
	const jobSegment = "job"
	const testJobName = "test"

	tests := []struct {
		name       string
		req        func() *http.Request
		statusCode int
	}{
		{
			name: "without job name",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/", nil)
			},
			statusCode: http.StatusNotFound,
		},
		{
			name: "job name not found",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/zfs/datasets/foobar", nil)
			},
			statusCode: http.StatusNotFound,
		},
		{
			name: "with job name",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/zfs/datasets/"+testJobName, nil)
			},
			statusCode: http.StatusOK,
		},
	}

	m := ExtractJobName(jobSegment, func(name string) bool {
		return name == testJobName
	})

	mux := http.NewServeMux()
	mux.Handle("/", AppendHandler([]Middleware{m},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))
	mux.Handle("/zfs/datasets/{job}", AppendHandler([]Middleware{m},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			jobName := JobNameFrom(r.Context())
			assert.Equal(t, testJobName, jobName)
			w.WriteHeader(http.StatusOK)
		})))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, tt.req())
			assert.Equal(t, tt.statusCode, resp.Code)
		})
	}
}
