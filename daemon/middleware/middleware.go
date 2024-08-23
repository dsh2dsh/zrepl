package middleware

import (
	"net/http"
	"slices"
)

func New(m ...Middleware) http.Handler {
	var next http.Handler
	for _, fn := range slices.Backward(m) {
		next = fn(next)
	}
	return next
}

type Middleware func(next http.Handler) http.Handler
