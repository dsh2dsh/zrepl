package job

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/client/jsonclient"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

func TestClient_Receive(t *testing.T) {
	testReq := pdu.ReceiveReq{Filesystem: "test/dataset"}
	testStream := []byte("foo\nbar\n")

	fn := func(_ context.Context, req *pdu.ReceiveReq, r io.ReadCloser) error {
		defer r.Close()
		assert.Equal(t, &testReq, req)
		b, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, testStream, b)
		return nil
	}

	ts := httptest.NewServer(middleware.Append(
		nil, middleware.JsonRequestStream(fn)))
	defer ts.Close()

	jsonClient, err := jsonclient.New(ts.URL)
	require.NoError(t, err)
	client := NewClient("test", jsonClient)
	err = client.Receive(t.Context(), &testReq,
		io.NopCloser(bytes.NewReader(testStream)))
	require.NoError(t, err)
}

func TestClient_Send(t *testing.T) {
	testReq := pdu.SendReq{Filesystem: "test/dataset"}
	testStream := []byte("foo\nbar\n")

	fn := func(_ context.Context, req *pdu.SendReq,
	) (*pdu.SendRes, io.ReadCloser, error) {
		assert.Equal(t, &testReq, req)
		return new(pdu.SendRes), io.NopCloser(bytes.NewBuffer(testStream)), nil
	}

	ts := httptest.NewServer(middleware.Append(
		nil, middleware.JsonRequestResponseStream(fn)))
	defer ts.Close()

	jsonClient, err := jsonclient.New(ts.URL)
	require.NoError(t, err)
	client := NewClient("test", jsonClient)

	_, r, err := client.Send(t.Context(), &testReq)
	require.NoError(t, err)
	defer r.Close()

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testStream, b)
}
