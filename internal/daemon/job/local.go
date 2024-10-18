package job

import "github.com/dsh2dsh/zrepl/internal/endpoint"

type makeReceiverFunc func(clientIdentity string) *endpoint.Receiver

var localListeners map[string]makeReceiverFunc

func init() {
	localListeners = make(map[string]makeReceiverFunc, 1)
}

func addLocalReceiver(listenerName string, fn makeReceiverFunc) {
	localListeners[listenerName] = fn
}

func getLocalReceiver(listenerName, clientIdentity string) *endpoint.Receiver {
	if fn := localListeners[listenerName]; fn != nil {
		return fn(clientIdentity)
	}
	return nil
}
