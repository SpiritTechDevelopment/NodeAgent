package xray

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	statsCommand "github.com/xtls/xray-core/app/stats/command"
)

func TestResetUsageQueriesOnceAndAggregatesManagedUsers(t *testing.T) {
	secondID := "u.bcdefghijklmnopqrstu"
	stats := &fakeStatsClient{
		queryResponse: &statsCommand.QueryStatsResponse{Stat: []*statsCommand.Stat{
			{Name: usageStatName(secondID, downlinkSuffix), Value: 23},
			{Name: "user>>>svc-monitoring>>>traffic>>>uplink", Value: 999},
			{Name: usageStatName(testAccountingID, uplinkSuffix), Value: 11},
			{Name: usageStatName(secondID, uplinkSuffix), Value: 0},
			{Name: usageStatName(testAccountingID, downlinkSuffix), Value: 17},
			{Name: "inbound>>>vless-in>>>traffic>>>uplink", Value: 1000},
		}},
	}
	client := newClient(io.NopCloser(nilReader{}), stats, &fakeRoutingClient{})

	usage, err := client.ResetUsage(context.Background())
	if err != nil {
		t.Fatalf("ResetUsage() вернул ошибку: %v", err)
	}
	want := []Usage{
		{AccountingID: testAccountingID, UplinkBytes: 11, DownlinkBytes: 17},
		{AccountingID: secondID, DownlinkBytes: 23},
	}
	if !slices.Equal(usage, want) {
		t.Fatalf("usage = %+v, ожидалось %+v", usage, want)
	}
	if stats.calls != 1 {
		t.Fatalf("QueryStats вызван %d раз, ожидался один", stats.calls)
	}
	if stats.queryRequest.GetPattern() != allUsersStatsPattern || !stats.queryRequest.GetReset_() {
		t.Fatalf("QueryStats request = %+v", stats.queryRequest)
	}
}

func TestResetUserUsageUsesExactPrefix(t *testing.T) {
	stats := &fakeStatsClient{
		queryResponse: &statsCommand.QueryStatsResponse{Stat: []*statsCommand.Stat{
			{Name: usageStatName(testAccountingID, downlinkSuffix), Value: 31},
		}},
	}
	client := newClient(io.NopCloser(nilReader{}), stats, &fakeRoutingClient{})

	usage, err := client.ResetUserUsage(context.Background(), testAccountingID)
	if err != nil {
		t.Fatalf("ResetUserUsage() вернул ошибку: %v", err)
	}
	if !slices.Equal(usage, []Usage{{AccountingID: testAccountingID, DownlinkBytes: 31}}) {
		t.Fatalf("usage = %+v", usage)
	}
	wantPattern := "user>>>" + testAccountingID + ">>>traffic>>>"
	if stats.queryRequest.GetPattern() != wantPattern || !stats.queryRequest.GetReset_() {
		t.Fatalf("QueryStats request = %+v, ожидался pattern %q с reset", stats.queryRequest, wantPattern)
	}
}

func TestResetUsageRejectsMalformedManagedCounters(t *testing.T) {
	tests := []struct {
		name  string
		stats []*statsCommand.Stat
	}{
		{name: "empty stat", stats: []*statsCommand.Stat{nil}},
		{
			name: "negative",
			stats: []*statsCommand.Stat{
				{Name: usageStatName(testAccountingID, uplinkSuffix), Value: -1},
			},
		},
		{
			name: "duplicate direction",
			stats: []*statsCommand.Stat{
				{Name: usageStatName(testAccountingID, uplinkSuffix), Value: 1},
				{Name: usageStatName(testAccountingID, uplinkSuffix), Value: 2},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stats := &fakeStatsClient{
				queryResponse: &statsCommand.QueryStatsResponse{Stat: test.stats},
			}
			client := newClient(io.NopCloser(nilReader{}), stats, &fakeRoutingClient{})
			if _, err := client.ResetUsage(context.Background()); err == nil {
				t.Fatal("ResetUsage() не отклонил некорректный ответ")
			}
		})
	}
}

func TestResetUsageValidatesAndWrapsFailures(t *testing.T) {
	wantErr := errors.New("stats unavailable")
	stats := &fakeStatsClient{err: wantErr}
	client := newClient(io.NopCloser(nilReader{}), stats, &fakeRoutingClient{})

	if _, err := client.ResetUsage(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("ResetUsage() error = %v, ожидалась исходная ошибка", err)
	}
	stats.err = nil
	if _, err := client.ResetUsage(context.Background()); err == nil {
		t.Error("ResetUsage() не отклонил пустой ответ")
	}
	if _, err := client.ResetUserUsage(context.Background(), "invalid"); err == nil {
		t.Error("ResetUserUsage() не отклонил accounting_id")
	}
	if stats.calls != 2 {
		t.Fatalf("QueryStats вызван %d раз, невалидный ID дошёл до RPC", stats.calls)
	}
}

func TestParseUsageStatsRejectsAnotherUserInPointResponse(t *testing.T) {
	stats := []*statsCommand.Stat{
		{Name: usageStatName("u.bcdefghijklmnopqrstu", uplinkSuffix), Value: 1},
	}
	if _, err := parseUsageStats(stats, testAccountingID); err == nil {
		t.Fatal("parseUsageStats() не отклонил другого пользователя")
	}
}

func usageStatName(accountingID, direction string) string {
	return allUsersStatsPattern + accountingID + trafficMarker + direction
}
