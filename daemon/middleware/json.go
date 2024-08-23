package middleware

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/dsh2dsh/zrepl/logger"
)

func JsonResponder(log logger.Logger, producer func() (any, error),
) Middleware {
	return func(next http.Handler) http.Handler {
		return NewJsonResponder(log, producer)
	}
}

func NewJsonResponder(log logger.Logger, producer func() (any, error),
) *jsonResponder {
	return &jsonResponder{log: log, producer: producer}
}

type jsonResponder struct {
	log      logger.Logger
	producer func() (any, error)
}

func (self *jsonResponder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res, err := self.producer()
	if err != nil {
		self.writeError(err, w, "control handler error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		self.writeError(err, w, "control handler json marshal error")
	}
}

func (self *jsonResponder) writeError(err error, w http.ResponseWriter,
	msg string,
) {
	self.log.WithError(err).Error(msg)
	w.WriteHeader(http.StatusInternalServerError)
	if _, err = io.WriteString(w, err.Error()); err != nil {
		self.log.WithError(err).Error("control handler io error")
	}
}

func JsonRequestResponder(log logger.Logger,
	producer func(decoder JsonDecoder) (any, error),
) Middleware {
	return func(next http.Handler) http.Handler {
		return NewJsonRequestResponder(log, producer)
	}
}

func NewJsonRequestResponder(log logger.Logger,
	producer func(decoder JsonDecoder) (any, error),
) *jsonRequestResponder {
	return &jsonRequestResponder{log: log, producer: producer}
}

type jsonRequestResponder struct {
	log      logger.Logger
	producer func(decoder JsonDecoder) (any, error)
}

type JsonDecoder = func(any) error

func (self *jsonRequestResponder) ServeHTTP(w http.ResponseWriter,
	r *http.Request,
) {
	var decodeErr error
	resp, err := self.producer(func(req any) error {
		decodeErr = json.NewDecoder(r.Body).Decode(&req)
		return decodeErr
	})

	// If we had a decode error ignore output of producer and return error
	if decodeErr != nil {
		self.writeError(decodeErr, w, "control handler json unmarshal error",
			http.StatusBadRequest)
		return
	} else if err != nil {
		self.writeError(err, w, "control handler error",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		self.writeError(err, w, "control handler json marshal error",
			http.StatusInternalServerError)
	}
}

func (self *jsonRequestResponder) writeError(err error, w http.ResponseWriter,
	msg string, statusCode int,
) {
	self.log.WithError(err).Error(msg)
	w.WriteHeader(statusCode)
	if _, err = io.WriteString(w, err.Error()); err != nil {
		self.log.WithError(err).Error("control handler io error")
	}
}
