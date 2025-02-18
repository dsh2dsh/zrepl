package jsonclient

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostStream(t *testing.T) {
	testData := struct{ Num int }{Num: 42}
	testStream := []byte("foo\nbar\n")

	httpClient := testHttpClient{
		doer: func(r *http.Request) (*http.Response, error) {
			defer r.Body.Close()
			in := testData
			in.Num = 0
			require.NoError(t, ReadJsonPayload(r.Header, r.Body, &in))
			assert.Equal(t, testData, in)

			b, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, testStream, b)

			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
			}
			return resp, nil
		},
	}

	client, err := New("http://server", WithHTTPClient(&httpClient))
	require.NoError(t, err)
	require.NoError(t, client.PostStream(t.Context(),
		"/", &testData, nil, bytes.NewBuffer(testStream)))
}

type testHttpClient struct {
	doer func(req *http.Request) (*http.Response, error)
}

func (self *testHttpClient) Do(req *http.Request) (*http.Response, error) {
	return self.doer(req)
}

func TestPostResponseStream(t *testing.T) {
	testData := struct{ Num int }{Num: 42}
	testStream := []byte("foo\nbar\n")

	httpClient := testHttpClient{
		doer: func(req *http.Request) (*http.Response, error) {
			in := testData
			in.Num = 0
			require.NoError(t, json.NewDecoder(req.Body).Decode(&in))
			assert.Equal(t, testData, in)
			require.NoError(t, req.Body.Close())

			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     map[string][]string{},
			}

			var b bytes.Buffer
			in.Num++
			require.NoError(t, WriteJsonPayload(resp.Header, &b, &in))

			resp.Body = io.NopCloser(
				io.MultiReader(&b, bytes.NewReader(testStream)))
			return resp, nil
		},
	}

	client, err := New("http://server", WithHTTPClient(&httpClient))
	require.NoError(t, err)

	out := testData
	r, err := client.PostResponseStream(t.Context(), "/", &testData, &out)
	require.NoError(t, err)
	defer r.Close()
	assert.Equal(t, testData.Num+1, out.Num)

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testStream, b)
}
