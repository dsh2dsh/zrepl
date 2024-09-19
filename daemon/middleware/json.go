package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/dsh2dsh/zrepl/logger"
)

func JsonResponder[T any](log logger.Logger, h func() (T, error)) Middleware {
	return func(next http.Handler) http.Handler {
		return &jsonResponder[T]{log: log, handler: h}
	}
}

type jsonResponder[T any] struct {
	log     logger.Logger
	handler func() (T, error)
}

func (self *jsonResponder[T]) ServeHTTP(w http.ResponseWriter,
	r *http.Request,
) {
	res, err := self.handler()
	if err != nil {
		self.writeError(err, w, "control handler error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&res); err != nil {
		self.writeError(err, w, "control handler json marshal error")
	}
}

func (self *jsonResponder[T]) writeError(err error, w http.ResponseWriter,
	msg string,
) {
	self.log.WithError(err).Error(msg)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// --------------------------------------------------

func JsonRequestResponder[T1 any, T2 any](log logger.Logger,
	h func(req *T1) (T2, error),
) Middleware {
	return func(next http.Handler) http.Handler {
		return &jsonRequestResponder[T1, T2]{log: log, handler: h}
	}
}

type jsonRequestResponder[T1 any, T2 any] struct {
	log     logger.Logger
	handler func(req *T1) (T2, error)
}

func (self *jsonRequestResponder[T1, T2]) ServeHTTP(w http.ResponseWriter,
	r *http.Request,
) {
	var req T1
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		self.writeError(err, w, "control handler json unmarshal error",
			http.StatusBadRequest)
		return
	}

	resp, err := self.handler(&req)
	if err != nil {
		self.writeError(err, w, "control handler error",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&resp); err != nil {
		self.writeError(err, w, "control handler json marshal error",
			http.StatusInternalServerError)
	}
}

func (self *jsonRequestResponder[T1, T2]) writeError(err error,
	w http.ResponseWriter, msg string, statusCode int,
) {
	self.log.WithError(err).Error(msg)
	http.Error(w, err.Error(), statusCode)
}
