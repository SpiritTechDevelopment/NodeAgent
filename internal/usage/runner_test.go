package usage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

func TestCollectionStatusTracksFailuresAndRecovery(t *testing.T) {
	store := openUsageStore(t)
	source := &fakeSource{}
	collector := newTestCollector(t, store, source)
	firstAt := time.Date(2026, time.August, 14, 17, 0, 0, 0, time.UTC)
	collector.now = func() time.Time { return firstAt }

	if _, err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("первый Collect() вернул ошибку: %v", err)
	}
	status := collector.Status()
	if !status.LastAttemptAt.Equal(firstAt) || !status.LastSuccessAt.Equal(firstAt) ||
		status.ConsecutiveFailures != 0 {
		t.Fatalf("status после успеха = %+v", status)
	}

	secondAt := firstAt.Add(time.Minute)
	collector.now = func() time.Time { return secondAt }
	source.err = errors.New("stats unavailable")
	if _, err := collector.Collect(context.Background()); err == nil {
		t.Fatal("Collect() не вернул ошибку source")
	}
	status = collector.Status()
	if !status.LastAttemptAt.Equal(secondAt) || !status.LastSuccessAt.Equal(firstAt) ||
		status.ConsecutiveFailures != 1 {
		t.Fatalf("status после ошибки = %+v", status)
	}

	source.err = nil
	if _, err := collector.Collect(context.Background()); err != nil {
		t.Fatalf("восстановительный Collect() вернул ошибку: %v", err)
	}
	status = collector.Status()
	if !status.LastSuccessAt.Equal(secondAt) || status.ConsecutiveFailures != 0 {
		t.Fatalf("status после восстановления = %+v", status)
	}
}

func TestRunContinuesAfterCollectionError(t *testing.T) {
	store := openUsageStore(t)
	source := &runnerSource{calls: make(chan int, 4), firstErr: errors.New("temporary stats failure")}
	collector := newTestCollector(t, store, source)
	collector.interval = 10 * time.Millisecond
	collector.phase = func(time.Duration) (time.Duration, error) { return 0, nil }
	reported := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- collector.Run(ctx, func(err error) { reported <- err })
	}()

	if call := <-source.calls; call != 1 {
		t.Fatalf("первый вызов = %d", call)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, source.firstErr) {
			t.Fatalf("report error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ошибка первого сбора не передана report")
	}
	select {
	case call := <-source.calls:
		if call != 2 {
			t.Fatalf("второй вызов = %d", call)
		}
	case <-time.After(time.Second):
		t.Fatal("цикл остановился после временной ошибки")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() после отмены вернул ошибку: %v", err)
	}
	if status := collector.Status(); status.ConsecutiveFailures != 0 || status.LastSuccessAt.IsZero() {
		t.Fatalf("status после восстановления = %+v", status)
	}
}

func TestRunRejectsSecondLoop(t *testing.T) {
	store := openUsageStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	source := &runBlockingSource{entered: entered, release: release}
	collector := newTestCollector(t, store, source)
	collector.phase = func(time.Duration) (time.Duration, error) { return 0, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, nil) }()
	<-entered

	if err := collector.Run(context.Background(), nil); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("второй Run() error = %v, ожидалась ErrAlreadyRunning", err)
	}
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("первый Run() вернул ошибку: %v", err)
	}
}

func TestRandomPhaseStaysWithinInterval(t *testing.T) {
	interval := 15 * time.Second
	for range 32 {
		phase, err := randomPhase(interval)
		if err != nil {
			t.Fatalf("randomPhase() вернул ошибку: %v", err)
		}
		if phase < 0 || phase >= interval {
			t.Fatalf("phase = %s вне [0, %s)", phase, interval)
		}
	}
	if _, err := randomPhase(0); err == nil {
		t.Fatal("randomPhase() не отклонил нулевой интервал")
	}
}

type runnerSource struct {
	mu       sync.Mutex
	count    int
	calls    chan int
	firstErr error
}

func (source *runnerSource) ResetUsage(context.Context) ([]xray.Usage, error) {
	source.mu.Lock()
	source.count++
	call := source.count
	source.mu.Unlock()
	source.calls <- call
	if call == 1 {
		return nil, source.firstErr
	}
	return nil, nil
}

func (source *runnerSource) ResetUserUsage(context.Context, string) ([]xray.Usage, error) {
	return nil, nil
}

type runBlockingSource struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (source *runBlockingSource) ResetUsage(context.Context) ([]xray.Usage, error) {
	close(source.entered)
	<-source.release
	return nil, nil
}

func (source *runBlockingSource) ResetUserUsage(context.Context, string) ([]xray.Usage, error) {
	return nil, nil
}
