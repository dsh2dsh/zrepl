package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type ctxKeyClientIdentity struct{}

var clientIdentityKey ctxKeyClientIdentity = struct{}{}

func ClientIdentityFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if name, ok := ctx.Value(clientIdentityKey).(string); ok {
		return name
	}
	return ""
}

func CheckClientIdentity(keys []config.AuthKey) Middleware {
	keyNames := make(map[string]string, len(keys))
	for i := range keys {
		key := &keys[i]
		keyNames[key.Key] = key.Name
	}
	m := &IdentityChecker{keys: keyNames}
	return m.middleware
}

type IdentityChecker struct {
	keys map[string]string
}

func (self *IdentityChecker) middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		log := getLogger(r)
		keyName := self.keyNameFrom(r)
		if keyName == "" {
			log.Error("access denied")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(self.context(r, keyName)))
	}
	return http.HandlerFunc(fn)
}

func (self *IdentityChecker) keyNameFrom(r *http.Request) string {
	log := getLogger(r)
	auth := r.Header.Get("Authorization")
	if auth == "" {
		log.Error("authorization header not found")
		return ""
	}

	token, foundToken := strings.CutPrefix(auth, "Bearer ")
	if !foundToken || token == "" {
		log.With(slog.String("authorization", auth)).
			Error("bearer token not found")
		return ""
	}

	keyName, ok := self.keys[token]
	if !ok {
		log.With(slog.String("token", token)).Error("client identity not found")
		return ""
	}
	return keyName
}

func (self *IdentityChecker) context(r *http.Request, clientIdentity string,
) context.Context {
	ctx := context.WithValue(r.Context(), clientIdentityKey, clientIdentity)
	return logging.WithLogger(ctx, getLogger(r).With(
		slog.String("client_identity", clientIdentity)))
}
