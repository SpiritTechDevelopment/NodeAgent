package xray

import (
	"context"
	"errors"
	"fmt"
	"time"

	statsCommand "github.com/xtls/xray-core/app/stats/command"
)

// Health возвращает uptime Xray и одновременно проверяет доступность StatsService.
func (c *Client) Health(ctx context.Context) (Health, error) {
	response, err := c.stats.GetSysStats(ctx, &statsCommand.SysStatsRequest{})
	if err != nil {
		return Health{}, fmt.Errorf("get Xray system stats: %w", err)
	}
	if response == nil {
		return Health{}, errors.New("get Xray system stats: empty response")
	}
	return Health{Uptime: time.Duration(response.GetUptime()) * time.Second}, nil
}
