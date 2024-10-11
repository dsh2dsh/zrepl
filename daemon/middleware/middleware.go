package middleware

import (
	"net/http"
	"slices"
)

type Middleware func(next http.Handler) http.Handler

func Append(m1 []Middleware, m2 ...Middleware) http.Handler {
	var next http.Handler
	for _, fn := range slices.Backward(m2) {
		next = fn(next)
	}

	if len(m1) != 0 {
		for _, fn := range slices.Backward(m1) {
			next = fn(next)
		}
	}
	return next
}

func AppendHandler(m []Middleware, h http.Handler) http.Handler {
	return Append(m, func(http.Handler) http.Handler { return h })
}
