package xray

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	proxymanCommand "github.com/xtls/xray-core/app/proxyman/command"
	routerCommand "github.com/xtls/xray-core/app/router/command"
	statsCommand "github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config задаёт адрес локального gRPC API Xray.
type Config struct {
	// Address содержит числовой loopback-адрес и порт, например 127.0.0.1:10085.
	Address string
	// InboundTag содержит тег клиентского VLESS inbound.
	InboundTag string
}

// Client реализует операции агента над локальным Xray.
type Client struct {
	connection io.Closer
	handler    handlerServiceClient
	stats      statsServiceClient
	routing    routingServiceClient
	inboundTag string
}

type handlerServiceClient interface {
	AlterInbound(
		context.Context,
		*proxymanCommand.AlterInboundRequest,
		...grpc.CallOption,
	) (*proxymanCommand.AlterInboundResponse, error)
	GetInboundUsers(
		context.Context,
		*proxymanCommand.GetInboundUserRequest,
		...grpc.CallOption,
	) (*proxymanCommand.GetInboundUserResponse, error)
}

type statsServiceClient interface {
	QueryStats(
		context.Context,
		*statsCommand.QueryStatsRequest,
		...grpc.CallOption,
	) (*statsCommand.QueryStatsResponse, error)
	GetSysStats(
		context.Context,
		*statsCommand.SysStatsRequest,
		...grpc.CallOption,
	) (*statsCommand.SysStatsResponse, error)
}

type routingServiceClient interface {
	AddRule(
		context.Context,
		*routerCommand.AddRuleRequest,
		...grpc.CallOption,
	) (*routerCommand.AddRuleResponse, error)
	RemoveRule(
		context.Context,
		*routerCommand.RemoveRuleRequest,
		...grpc.CallOption,
	) (*routerCommand.RemoveRuleResponse, error)
	ListRule(
		context.Context,
		*routerCommand.ListRuleRequest,
		...grpc.CallOption,
	) (*routerCommand.ListRuleResponse, error)
	TestRoute(
		context.Context,
		*routerCommand.TestRouteRequest,
		...grpc.CallOption,
	) (*routerCommand.RoutingContext, error)
}

// New создаёт клиент локального Xray API. Фактическое соединение устанавливается
// при первом RPC и ограничивается контекстом вызова.
func New(config Config) (*Client, error) {
	address, err := validateAddress(config.Address)
	if err != nil {
		return nil, err
	}
	inboundTag, err := validateInboundTag(config.InboundTag)
	if err != nil {
		return nil, err
	}

	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Xray gRPC client: %w", err)
	}
	return newClientWithHandler(
		connection,
		proxymanCommand.NewHandlerServiceClient(connection),
		statsCommand.NewStatsServiceClient(connection),
		routerCommand.NewRoutingServiceClient(connection),
		inboundTag,
	), nil
}

// Close закрывает gRPC-соединение с Xray.
func (c *Client) Close() error {
	return c.connection.Close()
}

func newClient(
	connection io.Closer,
	stats statsServiceClient,
	routing routingServiceClient,
) *Client {
	return newClientWithHandler(connection, nil, stats, routing, "")
}

func newClientWithHandler(
	connection io.Closer,
	handler handlerServiceClient,
	stats statsServiceClient,
	routing routingServiceClient,
	inboundTag string,
) *Client {
	return &Client{
		connection: connection,
		handler:    handler,
		stats:      stats,
		routing:    routing,
		inboundTag: inboundTag,
	}
}

func validateAddress(raw string) (string, error) {
	address := strings.TrimSpace(raw)
	if address == "" {
		return "", errors.New("Xray API address is required")
	}
	if address != raw {
		return "", errors.New("Xray API address must not contain surrounding whitespace")
	}

	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("parse Xray API address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", errors.New("Xray API address must use a numeric loopback host")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", errors.New("Xray API address must contain a valid non-zero port")
	}
	return address, nil
}

func validateInboundTag(raw string) (string, error) {
	inboundTag := strings.TrimSpace(raw)
	if inboundTag == "" {
		return "", errors.New("Xray inbound tag is required")
	}
	if inboundTag != raw {
		return "", errors.New("Xray inbound tag must not contain surrounding whitespace")
	}
	return inboundTag, nil
}
