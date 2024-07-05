package daemon

import (
	"net/http"
	"sync"

	// FIXME: importing this package has the side-effect of poisoning the http.DefaultServeMux
	// FIXME: with the /debug/pprof endpoints
	"context"
	"net"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/websocket"

	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
)

type pprofServer struct {
	cc       chan PprofServerControlMsg
	listener net.Listener
	wg       sync.WaitGroup
}

type PprofServerControlMsg struct {
	// Whether the server should listen for requests on the given address
	Run bool
	// Must be set if Run is true, undefined otherwise
	HttpListenAddress string
}

func NewPProfServer(ctx context.Context) *pprofServer {
	s := &pprofServer{
		cc: make(chan PprofServerControlMsg),
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.controlLoop(ctx)
	}()
	return s
}

func (s *pprofServer) controlLoop(ctx context.Context) {
	log := job.GetLogger(ctx)
	defer log.Info("pprof server exiting")

outer:
	for {

		var msg PprofServerControlMsg
		select {
		case <-ctx.Done():
			if s.listener != nil {
				s.listener.Close()
			}
			break outer
		case msg = <-s.cc:
			// proceed
		}

		var err error
		if msg.Run && s.listener == nil {

			s.listener, err = net.Listen("tcp", msg.HttpListenAddress)
			if err != nil {
				s.listener = nil
				continue
			}

			// FIXME: because net/http/pprof does not provide a mux,
			mux := http.NewServeMux()
			mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
			mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
			mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
			mux.Handle("/metrics", promhttp.Handler())
			mux.Handle("/debug/zrepl/activity-trace", websocket.Handler(trace.ChrometraceClientWebsocketHandler))
			go func() {
				err := http.Serve(s.listener, mux)
				if ctx.Err() != nil {
					return
				} else if err != nil {
					log.WithError(err).Error("pprof server serve error")
				}
			}()
			continue
		}

		if !msg.Run && s.listener != nil {
			s.listener.Close()
			s.listener = nil
			continue
		}
	}
}

func (s *pprofServer) Control(msg PprofServerControlMsg) {
	s.cc <- msg
}

func (s *pprofServer) Wait() { s.wg.Wait() }
