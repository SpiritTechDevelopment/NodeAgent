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

// ApplyRouting обязана присылать таблицу целиком: ShouldAppend=false очищает и
// правила, и балансировщики Xray, поэтому всё, чего нет в запросе, будет
// уничтожено — правила остальных пользователей, правила инфраструктуры и
// завершающий default-deny.
func TestApplyRoutingSendsCompleteTable(t *testing.T) {
	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	secondID := "u.bcdefghijklmnopqrstu"
	table := RoutingTable{config: &router.Config{Rule: []*router.RoutingRule{
		{
			TargetTag:  &router.RoutingRule_Tag{Tag: "api"},
			RuleTag:    "infra:api",
			InboundTag: []string{"api"},
		},
		{
			TargetTag: &router.RoutingRule_Tag{Tag: "direct"},
			RuleTag:   userRuleTagPrefix + testAccountingID,
			UserEmail: []string{testAccountingID},
		},
		{
			TargetTag: &router.RoutingRule_Tag{Tag: "bridge-test"},
			RuleTag:   userRuleTagPrefix + secondID,
			UserEmail: []string{secondID},
		},
		{
			TargetTag:  &router.RoutingRule_Tag{Tag: "block"},
			RuleTag:    defaultDenyRuleTagPrefix + "vless-in",
			InboundTag: []string{"vless-in"},
		},
	}}}
	if err := client.ApplyRouting(context.Background(), table); err != nil {
		t.Fatalf("ApplyRouting() вернул ошибку: %v", err)
	}

	request := routing.addRequest
	if request == nil {
		t.Fatal("AddRule не был вызван")
	}
	if request.GetShouldAppend() {
		t.Error("should_append=true: правила легли бы после default-deny")
	}
	if got := request.GetConfig().GetType(); got != "xray.app.router.Config" {
		t.Fatalf("тип config = %q, ожидался xray.app.router.Config", got)
	}
	instance, err := request.GetConfig().GetInstance()
	if err != nil {
		t.Fatalf("декодировать routing config: %v", err)
	}
	configuration, ok := instance.(*router.Config)
	if !ok {
		t.Fatalf("тип routing config = %T", instance)
	}
	gotTags := make([]string, 0, len(configuration.GetRule()))
	for _, rule := range configuration.GetRule() {
		gotTags = append(gotTags, rule.GetRuleTag())
	}
	wantTags := []string{
		"infra:api",
		userRuleTagPrefix + testAccountingID,
		userRuleTagPrefix + secondID,
		defaultDenyRuleTagPrefix + "vless-in",
	}
	if !slices.Equal(gotTags, wantTags) {
		t.Fatalf("отправленные rule_tag = %v, ожидались %v", gotTags, wantTags)
	}
}

func TestApplyRoutingRejectsEmptyTable(t *testing.T) {
	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)

	if err := client.ApplyRouting(context.Background(), RoutingTable{}); err == nil {
		t.Fatal("ApplyRouting() приняла пустую таблицу")
	}
	if routing.addRequest != nil {
		t.Error("пустая таблица дошла до Xray и стёрла бы всю маршрутизацию")
	}
}

func TestRoutingMethodsWrapFailures(t *testing.T) {
	wantErr := errors.New("routing unavailable")
	routing := &fakeRoutingClient{err: wantErr}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)
	ctx := context.Background()

	table := RoutingTable{config: &router.Config{Rule: []*router.RoutingRule{{
		TargetTag: &router.RoutingRule_Tag{Tag: "direct"},
		RuleTag:   userRuleTagPrefix + testAccountingID,
		UserEmail: []string{testAccountingID},
	}}}}
	if err := client.ApplyRouting(ctx, table); !errors.Is(err, wantErr) {
		t.Errorf("ApplyRouting() error = %v, ожидалась исходная ошибка", err)
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

	// accounting_id и outbound tag проверяет ConfigFile.Reconcile: таблицу теперь
	// собирает конфигурация, а не отдельный вызов клиента.
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
