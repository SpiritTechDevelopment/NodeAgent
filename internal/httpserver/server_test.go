package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
)

func TestHealthEndpoints(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		status     service.Status
		statusErr  error
		wantStatus int
		wantBody   string
	}{
		{name: "live", path: "/health/live", wantStatus: http.StatusOK, wantBody: "ok\n"},
		{name: "live alias", path: "/healthz", wantStatus: http.StatusOK, wantBody: "ok\n"},
		{
			name: "ready",
			path: "/health/ready",
			status: service.Status{
				StateWritable:       true,
				UsageCollectionSafe: true,
				Xray:                service.XrayStatus{Reachable: true},
			},
			wantStatus: http.StatusOK,
			wantBody:   "ready\n",
		},
		{
			name:       "bootstrap",
			path:       "/readyz",
			status:     service.Status{NeedsBootstrap: true},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "not ready\n",
		},
		{
			name:       "provider failure",
			path:       "/health/ready",
			statusErr:  errors.New("secret dependency detail"),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "not ready\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := newHandler(
				fixedStatus{status: test.status, err: test.statusErr},
				http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
					_, _ = response.Write([]byte("metric 1\n"))
				}),
				time.Second,
			)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != test.wantStatus || response.Body.String() != test.wantBody {
				t.Fatalf("status=%d body=%q, ожидалось status=%d body=%q",
					response.Code, response.Body.String(), test.wantStatus, test.wantBody)
			}
			if strings.Contains(response.Body.String(), "secret") {
				t.Fatal("ответ раскрыл внутреннюю диагностическую ошибку")
			}
		})
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	if _, err := New(nil, fixedStatus{}, http.NotFoundHandler()); err == nil {
		t.Fatal("New() принял nil listener")
	}
}

func TestServerServeAndShutdown(t *testing.T) {
	listener := newTestListener()
	server, err := New(listener, fixedStatus{}, http.NotFoundHandler())
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("Serve() не начал принимать соединения")
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() вернул ошибку: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() после Shutdown() вернул ошибку: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() не завершился после Shutdown()")
	}
}

func TestMetricsEndpointAndMethodRestriction(t *testing.T) {
	handler := newHandler(
		fixedStatus{},
		http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			_, _ = response.Write([]byte("spirit_agent_up 1\n"))
		}),
		time.Second,
	)
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsResponse.Code != http.StatusOK || metricsResponse.Body.String() != "spirit_agent_up 1\n" {
		t.Fatalf("metrics response: status=%d body=%q", metricsResponse.Code, metricsResponse.Body.String())
	}

	methodResponse := httptest.NewRecorder()
	handler.ServeHTTP(methodResponse, httptest.NewRequest(http.MethodPost, "/health/live", nil))
	if methodResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /health/live status = %d", methodResponse.Code)
	}
}

func TestReadinessHonorsTimeout(t *testing.T) {
	handler := newHandler(blockingStatus{}, http.NotFoundHandler(), time.Millisecond)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, ожидалось 503", response.Code)
	}
}

type fixedStatus struct {
	status service.Status
	err    error
}

func (status fixedStatus) Status(context.Context) (service.Status, error) {
	return status.status, status.err
}

type blockingStatus struct{}

func (blockingStatus) Status(ctx context.Context) (service.Status, error) {
	<-ctx.Done()
	return service.Status{}, ctx.Err()
}

type testListener struct {
	accepted   chan struct{}
	closed     chan struct{}
	acceptOnce sync.Once
	closeOnce  sync.Once
}

func newTestListener() *testListener {
	return &testListener{accepted: make(chan struct{}), closed: make(chan struct{})}
}

func (listener *testListener) Accept() (net.Conn, error) {
	listener.acceptOnce.Do(func() { close(listener.accepted) })
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *testListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (*testListener) Addr() net.Addr {
	return testAddress("local-test")
}

type testAddress string

func (address testAddress) Network() string { return string(address) }
func (address testAddress) String() string  { return string(address) }
