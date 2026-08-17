package usage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	maxItemsPerBatch          = 5000
	defaultCollectionInterval = 15 * time.Second
)

var (
	// ErrAlreadyRunning означает, что периодический цикл collector уже запущен.
	ErrAlreadyRunning = errors.New("usage collector is already running")
)

// Source читает и сбрасывает счётчики пользователей в Xray.
type Source interface {
	ResetUsage(context.Context) ([]xray.Usage, error)
	ResetUserUsage(context.Context, string) ([]xray.Usage, error)
}

// Collector сериализует сбросы Xray и сохраняет результат в SQLite outbox.
type Collector struct {
	state    *state.Store
	source   Source
	mu       sync.Mutex
	statusMu sync.RWMutex
	status   CollectionStatus
	running  atomic.Bool
	now      func() time.Time
	phase    func(time.Duration) (time.Duration, error)
	interval time.Duration
	maxItems int
}

// CollectionStatus содержит потокобезопасный снимок периодического bulk-сбора.
type CollectionStatus struct {
	// LastAttemptAt содержит время последней завершённой попытки.
	LastAttemptAt time.Time
	// LastSuccessAt содержит время последнего успешного сбора, включая пустой.
	LastSuccessAt time.Time
	// ConsecutiveFailures содержит число последовательных ошибок.
	ConsecutiveFailures uint64
}

// New создаёт сборщик трафика с лимитом 5000 элементов на batch.
func New(store *state.Store, source Source) (*Collector, error) {
	if store == nil {
		return nil, errors.New("state store is required")
	}
	if source == nil {
		return nil, errors.New("Xray usage source is required")
	}
	return &Collector{
		state:    store,
		source:   source,
		now:      time.Now,
		phase:    randomPhase,
		interval: defaultCollectionInterval,
		maxItems: maxItemsPerBatch,
	}, nil
}

// Collect выполняет один bulk-сброс и атомарно сохраняет созданные batch.
func (c *Collector) Collect(ctx context.Context) ([]state.UsageBatch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	batches, err := c.collect(ctx, c.source.ResetUsage)
	c.recordCollection(err)
	return batches, err
}

// Flush выполняет внеочередной bulk-сброс трафика перед массовыми мутациями.
func (c *Collector) Flush(ctx context.Context) error {
	_, err := c.Collect(ctx)
	return err
}

// Status возвращает согласованный снимок состояния bulk-сбора.
func (c *Collector) Status() CollectionStatus {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

// FinalizeUser сохраняет остаток трафика пользователя перед его заменой или удалением.
func (c *Collector) FinalizeUser(ctx context.Context, accountingID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.collect(ctx, func(ctx context.Context) ([]xray.Usage, error) {
		return c.source.ResetUserUsage(ctx, accountingID)
	})
	return err
}

func (c *Collector) collect(
	ctx context.Context,
	reset func(context.Context) ([]xray.Usage, error),
) ([]state.UsageBatch, error) {
	if err := c.state.Writable(ctx); err != nil {
		return nil, fmt.Errorf("verify usage outbox before Xray reset: %w", err)
	}
	items, err := reset(ctx)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	payloads, err := c.payloads(items)
	if err != nil {
		return nil, err
	}
	batches, err := c.state.AppendUsageBatches(ctx, c.now().UTC(), payloads)
	if err != nil {
		return nil, fmt.Errorf("persist reset Xray usage: %w", err)
	}
	return batches, nil
}

func (c *Collector) payloads(items []xray.Usage) ([][]byte, error) {
	if c.maxItems <= 0 {
		return nil, errors.New("usage batch size must be positive")
	}
	ordered := append([]xray.Usage(nil), items...)
	slices.SortFunc(ordered, func(left, right xray.Usage) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	for index, item := range ordered {
		if err := xray.ValidateAccountingID(item.AccountingID); err != nil {
			return nil, fmt.Errorf("validate usage item %d: %w", index, err)
		}
		if item.UplinkBytes == 0 && item.DownlinkBytes == 0 {
			return nil, fmt.Errorf("usage item %d has no traffic", index)
		}
		if index > 0 && ordered[index-1].AccountingID == item.AccountingID {
			return nil, errors.New("usage collection contains duplicate accounting ID")
		}
	}

	payloads := make([][]byte, 0, (len(ordered)+c.maxItems-1)/c.maxItems)
	for start := 0; start < len(ordered); start += c.maxItems {
		end := min(start+c.maxItems, len(ordered))
		message := &nodeagentv1.UsageBatch{
			Items: make([]*nodeagentv1.UserUsage, 0, end-start),
		}
		for _, item := range ordered[start:end] {
			message.Items = append(message.Items, &nodeagentv1.UserUsage{
				AccountingId:  item.AccountingID,
				UplinkBytes:   item.UplinkBytes,
				DownlinkBytes: item.DownlinkBytes,
			})
		}
		payload, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("encode usage batch: %w", err)
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
}

func (c *Collector) recordCollection(err error) {
	observedAt := c.now().UTC()
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status.LastAttemptAt = observedAt
	if err == nil {
		c.status.LastSuccessAt = observedAt
		c.status.ConsecutiveFailures = 0
		return
	}
	c.status.ConsecutiveFailures++
}
