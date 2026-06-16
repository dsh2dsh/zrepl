package middleware

import (
	"context"
	"errors"
	"net/http"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

func NoContent(h func(context.Context) error) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if err := h(r.Context()); err != nil {
			writeError(w, r, err, "handler error")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}

func writeError(w http.ResponseWriter, r *http.Request, err error, msg string,
) {
	statusCode := http.StatusInternalServerError
	if httpErr, ok := errors.AsType[*HttpError](err); ok {
		statusCode = httpErr.StatusCode()
	}
	writeErrorCode(w, r, statusCode, err, msg)
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, statusCode int,
	err error, msg string,
) {
	logger.WithError(getLogger(r), err, msg)
	http.Error(w, err.Error(), statusCode)
}
