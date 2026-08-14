package usage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// Run запускает bulk-сбор после случайной начальной фазы и повторяет его каждые
// 15 секунд. Ошибки отдельных попыток передаются report и не останавливают цикл.
func (c *Collector) Run(ctx context.Context, report func(error)) error {
	if !c.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer c.running.Store(false)

	if c.interval <= 0 {
		return errors.New("usage collection interval must be positive")
	}
	phase, err := c.phase(c.interval)
	if err != nil {
		return fmt.Errorf("choose usage collection phase: %w", err)
	}
	if err := waitForCollection(ctx, phase); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		if _, err := c.Collect(ctx); err != nil && ctx.Err() == nil && report != nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func waitForCollection(ctx context.Context, delay time.Duration) error {
	if delay < 0 {
		return errors.New("usage collection phase must not be negative")
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return nil
	}
}

func randomPhase(interval time.Duration) (time.Duration, error) {
	if interval <= 0 {
		return 0, errors.New("usage collection interval must be positive")
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(interval)))
	if err != nil {
		return 0, err
	}
	return time.Duration(value.Int64()), nil
}
