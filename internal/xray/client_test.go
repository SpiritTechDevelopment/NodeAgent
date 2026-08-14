package xray

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/router"
	routerCommand "github.com/xtls/xray-core/app/router/command"
	statsCommand "github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testAccountingID = "u.abcdefghijklmnopqrst"

func TestNewValidatesLoopbackAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{name: "IPv4 loopback", address: "127.0.0.1:10085"},
		{name: "IPv6 loopback", address: "[::1]:10085"},
		{name: "empty", wantErr: true},
		{name: "whitespace", address: " 127.0.0.1:10085", wantErr: true},
		{name: "hostname", address: "localhost:10085", wantErr: true},
		{name: "non-loopback", address: "192.0.2.10:10085", wantErr: true},
		{name: "missing port", address: "127.0.0.1", wantErr: true},
		{name: "zero port", address: "127.0.0.1:0", wantErr: true},
		{name: "invalid port", address: "127.0.0.1:not-a-port", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := New(Config{Address: test.address, InboundTag: "vless-in"})
			if test.wantErr {
				if err == nil {
					_ = client.Close()
					t.Fatal("New() не вернул ожидаемую ошибку")
				}
				return
			}
			if err != nil {
				t.Fatalf("New() вернул ошибку: %v", err)
			}
			if err := client.Close(); err != nil {
				t.Fatalf("Close() вернул ошибку: %v", err)
			}
		})
	}
}

func TestNewValidatesInboundTag(t *testing.T) {
	for _, inboundTag := range []string{"", " ", " vless-in", "vless-in "} {
		client, err := New(Config{Address: "127.0.0.1:10085", InboundTag: inboundTag})
		if err == nil {
			_ = client.Close()
			t.Errorf("New() не отклонил inbound tag %q", inboundTag)
		}
	}
}

func TestHealthReturnsXrayUptime(t *testing.T) {
	stats := &fakeStatsClient{
		response: &statsCommand.SysStatsResponse{Uptime: 37},
	}
	client := newClient(io.NopCloser(nilReader{}), stats, &fakeRoutingClient{})

	health, err := client.Health(context.Background())
	if err != nil {
		t.Fatalf("Health() вернул ошибку: %v", err)
	}
	if health.Uptime != 37*time.Second {
		t.Errorf("uptime = %v, ожидалось 37s", health.Uptime)
	}
	if stats.calls != 1 {
		t.Errorf("GetSysStats вызван %d раз, ожидался один вызов", stats.calls)
	}
}

func TestHealthWrapsServiceFailures(t *testing.T) {
	wantErr := errors.New("stats unavailable")
	client := newClient(
		io.NopCloser(nilReader{}),
		&fakeStatsClient{err: wantErr},
		&fakeRoutingClient{},
	)
	if _, err := client.Health(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Health() error = %v, ожидалась исходная ошибка", err)
	}

	client = newClient(
		io.NopCloser(nilReader{}),
		&fakeStatsClient{},
		&fakeRoutingClient{},
	)
	if _, err := client.Health(context.Background()); err == nil {
		t.Fatal("Health() не отклонил пустой ответ")
	}
}

func TestAddUserRuleBuildsAppendOnlyRoutingRule(t *testing.T) {
	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	if err := client.AddUserRule(context.Background(), testAccountingID, "direct"); err != nil {
		t.Fatalf("AddUserRule() вернул ошибку: %v", err)
	}
	request := routing.addRequest
	if request == nil {
		t.Fatal("AddRule не был вызван")
	}
	if !request.GetShouldAppend() {
		t.Error("should_append=false, ожидалось true")
	}
	if got := request.GetConfig().GetType(); got != "xray.app.router.Config" {
		t.Fatalf("тип config = %q, ожидался xray.app.router.Config", got)
	}

	instance, err := request.GetConfig().GetInstance()
	if err != nil {
		t.Fatalf("декодировать routing rule: %v", err)
	}
	configuration, ok := instance.(*router.Config)
	if !ok {
		t.Fatalf("тип routing config = %T", instance)
	}
	if len(configuration.GetRule()) != 1 {
		t.Fatalf("число routing rules = %d, ожидалось 1", len(configuration.GetRule()))
	}
	rule := configuration.GetRule()[0]
	wantRuleTag := "spirit-agent:user:" + testAccountingID
	if rule.GetRuleTag() != wantRuleTag {
		t.Errorf("rule_tag = %q, ожидался %q", rule.GetRuleTag(), wantRuleTag)
	}
	if rule.GetTag() != "direct" {
		t.Errorf("outbound tag = %q, ожидался direct", rule.GetTag())
	}
	if !slices.Equal(rule.GetUserEmail(), []string{testAccountingID}) {
		t.Errorf("user_email = %v, ожидался %q", rule.GetUserEmail(), testAccountingID)
	}
	if len(rule.GetInboundTag()) != 0 {
		t.Errorf("inbound_tag = %v, ожидался пустой фильтр", rule.GetInboundTag())
	}
}

func TestRoutingMethodsWrapFailures(t *testing.T) {
	wantErr := errors.New("routing unavailable")
	routing := &fakeRoutingClient{err: wantErr}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)
	ctx := context.Background()

	if err := client.AddUserRule(ctx, testAccountingID, "direct"); !errors.Is(err, wantErr) {
		t.Errorf("AddUserRule() error = %v, ожидалась исходная ошибка", err)
	}
	if err := client.RemoveUserRule(ctx, testAccountingID); !errors.Is(err, wantErr) {
		t.Errorf("RemoveUserRule() error = %v, ожидалась исходная ошибка", err)
	}
	if _, err := client.UserRules(ctx); !errors.Is(err, wantErr) {
		t.Errorf("UserRules() error = %v, ожидалась исходная ошибка", err)
	}
	if _, err := client.TestUserRoute(ctx, testAccountingID); !errors.Is(err, wantErr) {
		t.Errorf("TestUserRoute() error = %v, ожидалась исходная ошибка", err)
	}
}

func TestRemoveUserRuleUsesDeterministicTag(t *testing.T) {
	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	if err := client.RemoveUserRule(context.Background(), testAccountingID); err != nil {
		t.Fatalf("RemoveUserRule() вернул ошибку: %v", err)
	}
	want := "spirit-agent:user:" + testAccountingID
	if got := routing.removeRequest.GetRuleTag(); got != want {
		t.Errorf("rule_tag = %q, ожидался %q", got, want)
	}
}

func TestUserRulesFiltersAndSortsAgentRules(t *testing.T) {
	secondID := "u.bcdefghijklmnopqrstu"
	routing := &fakeRoutingClient{
		listResponse: &routerCommand.ListRuleResponse{Rules: []*routerCommand.ListRuleItem{
			{Tag: "bridge", RuleTag: userRuleTagPrefix + secondID},
			nil,
			{Tag: "api", RuleTag: "infrastructure:api"},
			{Tag: "direct", RuleTag: userRuleTagPrefix + testAccountingID},
			{Tag: "block", RuleTag: userRuleTagPrefix + "invalid"},
		}},
	}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	rules, err := client.UserRules(context.Background())
	if err != nil {
		t.Fatalf("UserRules() вернул ошибку: %v", err)
	}
	wantIDs := []string{testAccountingID, secondID}
	gotIDs := []string{rules[0].AccountingID, rules[1].AccountingID}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Errorf("accounting IDs = %v, ожидались %v", gotIDs, wantIDs)
	}
	if rules[0].OutboundTag != "direct" || rules[1].OutboundTag != "bridge" {
		t.Errorf("outbound tags = %q, %q", rules[0].OutboundTag, rules[1].OutboundTag)
	}

	routing.listResponse = nil
	if _, err := client.UserRules(context.Background()); err == nil {
		t.Fatal("UserRules() не отклонил пустой ответ")
	}
}

func TestTestUserRouteUsesOnlyUserContext(t *testing.T) {
	routing := &fakeRoutingClient{
		testResponse: &routerCommand.RoutingContext{OutboundTag: "direct"},
	}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	outbound, err := client.TestUserRoute(context.Background(), testAccountingID)
	if err != nil {
		t.Fatalf("TestUserRoute() вернул ошибку: %v", err)
	}
	if outbound != "direct" {
		t.Errorf("outbound = %q, ожидался direct", outbound)
	}
	request := routing.testRequest
	if request.GetRoutingContext().GetUser() != testAccountingID {
		t.Errorf("user = %q, ожидался %q", request.GetRoutingContext().GetUser(), testAccountingID)
	}
	if !slices.Equal(request.GetFieldSelectors(), []string{"outbound"}) {
		t.Errorf("field selectors = %v, ожидался outbound", request.GetFieldSelectors())
	}
	if request.GetPublishResult() {
		t.Error("publish_result=true, ожидалось false")
	}

	routing.testResponse = &routerCommand.RoutingContext{}
	if _, err := client.TestUserRoute(context.Background(), testAccountingID); err == nil {
		t.Fatal("TestUserRoute() не отклонил пустой outbound")
	}

	routing.err = status.Error(codes.Unknown, "not enough information for making a decision")
	if _, err := client.TestUserRoute(context.Background(), testAccountingID); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("TestUserRoute() error = %v, ожидалась ErrRouteNotFound", err)
	}
}

func TestUserRuleValidationStopsBeforeRPC(t *testing.T) {
	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)
	ctx := context.Background()

	if err := client.AddUserRule(ctx, "invalid", "direct"); err == nil {
		t.Error("AddUserRule() не отклонил accounting_id")
	}
	if err := client.AddUserRule(ctx, testAccountingID, " "); err == nil {
		t.Error("AddUserRule() не отклонил пустой outbound tag")
	}
	if err := client.AddUserRule(ctx, testAccountingID, " direct"); err == nil {
		t.Error("AddUserRule() не отклонил пробел в outbound tag")
	}
	if err := client.RemoveUserRule(ctx, "invalid"); err == nil {
		t.Error("RemoveUserRule() не отклонил accounting_id")
	}
	if _, err := client.TestUserRoute(ctx, "invalid"); err == nil {
		t.Error("TestUserRoute() не отклонил accounting_id")
	}
	if routing.calls != 0 {
		t.Errorf("routing RPC вызван %d раз для невалидных данных", routing.calls)
	}
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

type fakeStatsClient struct {
	response      *statsCommand.SysStatsResponse
	queryResponse *statsCommand.QueryStatsResponse
	queryRequest  *statsCommand.QueryStatsRequest
	err           error
	calls         int
}

func (f *fakeStatsClient) QueryStats(
	_ context.Context,
	request *statsCommand.QueryStatsRequest,
	_ ...grpc.CallOption,
) (*statsCommand.QueryStatsResponse, error) {
	f.calls++
	f.queryRequest = request
	return f.queryResponse, f.err
}

func (f *fakeStatsClient) GetSysStats(
	context.Context,
	*statsCommand.SysStatsRequest,
	...grpc.CallOption,
) (*statsCommand.SysStatsResponse, error) {
	f.calls++
	return f.response, f.err
}

type fakeRoutingClient struct {
	addRequest    *routerCommand.AddRuleRequest
	removeRequest *routerCommand.RemoveRuleRequest
	listResponse  *routerCommand.ListRuleResponse
	testRequest   *routerCommand.TestRouteRequest
	testResponse  *routerCommand.RoutingContext
	err           error
	calls         int
}

func (f *fakeRoutingClient) AddRule(
	_ context.Context,
	request *routerCommand.AddRuleRequest,
	_ ...grpc.CallOption,
) (*routerCommand.AddRuleResponse, error) {
	f.calls++
	f.addRequest = request
	return &routerCommand.AddRuleResponse{}, f.err
}

func (f *fakeRoutingClient) RemoveRule(
	_ context.Context,
	request *routerCommand.RemoveRuleRequest,
	_ ...grpc.CallOption,
) (*routerCommand.RemoveRuleResponse, error) {
	f.calls++
	f.removeRequest = request
	return &routerCommand.RemoveRuleResponse{}, f.err
}

func (f *fakeRoutingClient) ListRule(
	context.Context,
	*routerCommand.ListRuleRequest,
	...grpc.CallOption,
) (*routerCommand.ListRuleResponse, error) {
	f.calls++
	return f.listResponse, f.err
}

func (f *fakeRoutingClient) TestRoute(
	_ context.Context,
	request *routerCommand.TestRouteRequest,
	_ ...grpc.CallOption,
) (*routerCommand.RoutingContext, error) {
	f.calls++
	f.testRequest = request
	return f.testResponse, f.err
}
