package xray

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	statsCommand "github.com/xtls/xray-core/app/stats/command"
)

const (
	allUsersStatsPattern = "user>>>"
	trafficMarker        = ">>>traffic>>>"
	uplinkSuffix         = "uplink"
	downlinkSuffix       = "downlink"
)

// ResetUsage одним RPC читает и сбрасывает счётчики всех пользователей Xray.
// В результат входят только backend-owned accounting_id и ненулевые дельты.
func (c *Client) ResetUsage(ctx context.Context) ([]Usage, error) {
	return c.queryUsage(ctx, allUsersStatsPattern, "")
}

// ResetUserUsage читает и сбрасывает счётчики одного backend-owned пользователя.
func (c *Client) ResetUserUsage(ctx context.Context, accountingID string) ([]Usage, error) {
	if err := ValidateAccountingID(accountingID); err != nil {
		return nil, err
	}
	pattern := allUsersStatsPattern + accountingID + trafficMarker
	return c.queryUsage(ctx, pattern, accountingID)
}

func (c *Client) queryUsage(
	ctx context.Context,
	pattern string,
	expectedAccountingID string,
) ([]Usage, error) {
	response, err := c.stats.QueryStats(ctx, &statsCommand.QueryStatsRequest{
		Pattern: pattern,
		Reset_:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("query and reset Xray user stats: %w", err)
	}
	if response == nil {
		return nil, errors.New("query and reset Xray user stats: empty response")
	}
	usage, err := parseUsageStats(response.GetStat(), expectedAccountingID)
	if err != nil {
		return nil, fmt.Errorf("parse reset Xray user stats: %w", err)
	}
	return usage, nil
}

func parseUsageStats(stats []*statsCommand.Stat, expectedAccountingID string) ([]Usage, error) {
	byUser := make(map[string]*Usage)
	seenDirections := make(map[string]struct{})
	for index, stat := range stats {
		if stat == nil {
			return nil, fmt.Errorf("stat %d is empty", index)
		}
		accountingID, direction, managed := parseUsageStatName(stat.GetName())
		if !managed {
			continue
		}
		if expectedAccountingID != "" && accountingID != expectedAccountingID {
			return nil, errors.New("single-user query returned another accounting ID")
		}
		if stat.GetValue() < 0 {
			return nil, errors.New("traffic counter must not be negative")
		}
		directionKey := accountingID + "\x00" + direction
		if _, exists := seenDirections[directionKey]; exists {
			return nil, errors.New("traffic response contains a duplicate direction")
		}
		seenDirections[directionKey] = struct{}{}

		item := byUser[accountingID]
		if item == nil {
			item = &Usage{AccountingID: accountingID}
			byUser[accountingID] = item
		}
		switch direction {
		case uplinkSuffix:
			item.UplinkBytes = uint64(stat.GetValue())
		case downlinkSuffix:
			item.DownlinkBytes = uint64(stat.GetValue())
		}
	}

	usage := make([]Usage, 0, len(byUser))
	for _, item := range byUser {
		if item.UplinkBytes == 0 && item.DownlinkBytes == 0 {
			continue
		}
		usage = append(usage, *item)
	}
	slices.SortFunc(usage, func(left, right Usage) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	return usage, nil
}

func parseUsageStatName(name string) (string, string, bool) {
	remainder, found := strings.CutPrefix(name, allUsersStatsPattern)
	if !found {
		return "", "", false
	}
	accountingID, direction, found := strings.Cut(remainder, trafficMarker)
	if !found || (direction != uplinkSuffix && direction != downlinkSuffix) {
		return "", "", false
	}
	if ValidateAccountingID(accountingID) != nil {
		return "", "", false
	}
	return accountingID, direction, true
}
