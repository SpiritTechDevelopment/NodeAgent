package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAppRunStopsComponentsAndClosesResources(t *testing.T) {
	firstServer := newFakeManagedServer()
	secondServer := newFakeManagedServer()
	workerStarted := make(chan struct{})
	closer := new(fakeCloser)
	app := &App{
		servers: []server{
			{name: "first server", managed: firstServer},
			{name: "second server", managed: secondServer},
		},
		workers: []worker{{
			name: "test worker",
			run: func(ctx context.Context, _ func(error)) error {
				close(workerStarted)
				<-ctx.Done()
				return nil
			},
		}},
		closers:         []io.Closer{closer},
		shutdownTimeout: time.Second,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	waitChannel(t, firstServer.started)
	waitChannel(t, secondServer.started)
	waitChannel(t, workerStarted)
	if err := app.Run(ctx); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("повторный Run() error = %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() завершился с ошибкой: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() не завершился после отмены")
	}
	if firstServer.shutdownCalls.Load() != 1 || secondServer.shutdownCalls.Load() != 1 {
		t.Fatalf("Shutdown() calls = %d и %d",
			firstServer.shutdownCalls.Load(), secondServer.shutdownCalls.Load())
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close() вернул ошибку: %v", err)
	}
	if err := app.Close(); err != nil || closer.calls.Load() != 1 {
		t.Fatalf("повторный Close(): error=%v calls=%d", err, closer.calls.Load())
	}
}

func TestAppRunPropagatesUnexpectedWorkerFailure(t *testing.T) {
	managed := newFakeManagedServer()
	wantErr := errors.New("worker failed")
	app := &App{
		servers: []server{{name: "test server", managed: managed}},
		workers: []worker{{
			name: "broken worker",
			run:  func(context.Context, func(error)) error { return wantErr },
		}},
		shutdownTimeout: time.Second,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err := app.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v", err)
	}
	if managed.shutdownCalls.Load() != 1 {
		t.Fatalf("Shutdown() calls = %d", managed.shutdownCalls.Load())
	}
}

type fakeManagedServer struct {
	started       chan struct{}
	stopped       chan struct{}
	stopOnce      sync.Once
	shutdownCalls atomic.Int64
}

func newFakeManagedServer() *fakeManagedServer {
	return &fakeManagedServer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (server *fakeManagedServer) Serve() error {
	close(server.started)
	<-server.stopped
	return nil
}

func (server *fakeManagedServer) Shutdown(context.Context) error {
	server.shutdownCalls.Add(1)
	server.stopOnce.Do(func() { close(server.stopped) })
	return nil
}

type fakeCloser struct {
	calls atomic.Int64
}

func (closer *fakeCloser) Close() error {
	closer.calls.Add(1)
	return nil
}

func waitChannel(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatal("компонент не запустился")
	}
}
