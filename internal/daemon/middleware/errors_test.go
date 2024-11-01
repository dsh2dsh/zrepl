package middleware

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHttpError(t *testing.T) {
	errOops := errors.New("oops")
	statusCode := http.StatusNotFound
	err := NewHttpError(statusCode, errOops)
	require.ErrorIs(t, err, errOops)

	var httpErr *HttpError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, statusCode, httpErr.StatusCode())
}
