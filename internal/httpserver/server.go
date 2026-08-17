// Package httpserver обслуживает локальные health-check и Prometheus-метрики.
package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
)

const (
	defaultReadinessTimeout = 2 * time.Second
	readHeaderTimeout       = 5 * time.Second
	idleTimeout             = 30 * time.Second
)

// Server обслуживает служебный HTTP listener агента.
type Server struct {
	listener net.Listener
	server   *http.Server
}

// New создаёт HTTP-сервер для liveness, readiness и Prometheus-метрик.
func New(
	listener net.Listener,
	status service.StatusProvider,
	metrics http.Handler,
) (*Server, error) {
	if listener == nil {
		return nil, errors.New("HTTP listener is required")
	}
	if status == nil {
		return nil, errors.New("status provider is required")
	}
	if metrics == nil {
		return nil, errors.New("metrics handler is required")
	}
	return &Server{
		listener: listener,
		server: &http.Server{
			Handler:           newHandler(status, metrics, defaultReadinessTimeout),
			ReadHeaderTimeout: readHeaderTimeout,
			IdleTimeout:       idleTimeout,
		},
	}, nil
}

// Serve начинает обслуживать служебный listener и блокируется до остановки.
func (server *Server) Serve() error {
	err := server.server.Serve(server.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown останавливает приём запросов и ждёт завершения активных обработчиков.
func (server *Server) Shutdown(ctx context.Context) error {
	return server.server.Shutdown(ctx)
}

func newHandler(
	status service.StatusProvider,
	metrics http.Handler,
	readinessTimeout time.Duration,
) http.Handler {
	mux := http.NewServeMux()
	live := func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		if request.Method == http.MethodGet {
			_, _ = response.Write([]byte("ok\n"))
		}
	}
	ready := func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		ctx, cancel := context.WithTimeout(request.Context(), readinessTimeout)
		defer cancel()
		snapshot, err := status.Status(ctx)
		if err != nil || !snapshot.Ready() {
			response.WriteHeader(http.StatusServiceUnavailable)
			if request.Method == http.MethodGet {
				_, _ = response.Write([]byte("not ready\n"))
			}
			return
		}
		response.WriteHeader(http.StatusOK)
		if request.Method == http.MethodGet {
			_, _ = response.Write([]byte("ready\n"))
		}
	}

	mux.HandleFunc("/health/live", live)
	mux.HandleFunc("/health/ready", ready)
	mux.HandleFunc("/healthz", live)
	mux.HandleFunc("/readyz", ready)
	mux.Handle("/metrics", metrics)
	return mux
}
