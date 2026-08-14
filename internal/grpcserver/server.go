package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
)

// Config задаёт транспортные лимиты и параметры взаимной TLS-аутентификации.
type Config struct {
	// TLS задаёт сертификат сервера и разрешённые идентичности backend.
	TLS TLSConfig
	// MaxReceiveMessageBytes ограничивает размер входящего gRPC-сообщения.
	// Ноль оставляет значение gRPC по умолчанию.
	MaxReceiveMessageBytes int
	// MaxSendMessageBytes ограничивает размер исходящего gRPC-сообщения.
	// Ноль оставляет значение gRPC по умолчанию.
	MaxSendMessageBytes int
}

// Server владеет gRPC-сервером и сетевым слушателем, принимающим запросы.
type Server struct {
	listener net.Listener
	server   *grpc.Server
}

// New создаёт gRPC-сервер с взаимной TLS-аутентификацией и регистрирует service.
// При успехе Server принимает владение listener. Если New возвращает ошибку,
// вызывающая сторона должна закрыть listener.
func New(
	listener net.Listener,
	config Config,
	service nodeagentv1.NodeAgentServiceServer,
) (*Server, error) {
	if listener == nil {
		return nil, errors.New("gRPC listener is required")
	}
	if service == nil {
		return nil, errors.New("node agent service is required")
	}
	if config.MaxReceiveMessageBytes < 0 {
		return nil, errors.New("maximum receive message size must not be negative")
	}
	if config.MaxSendMessageBytes < 0 {
		return nil, errors.New("maximum send message size must not be negative")
	}

	tlsConfig, err := config.TLS.Load()
	if err != nil {
		return nil, fmt.Errorf("configure gRPC TLS: %w", err)
	}

	options := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
	}
	if config.MaxReceiveMessageBytes > 0 {
		options = append(options, grpc.MaxRecvMsgSize(config.MaxReceiveMessageBytes))
	}
	if config.MaxSendMessageBytes > 0 {
		options = append(options, grpc.MaxSendMsgSize(config.MaxSendMessageBytes))
	}

	server := grpc.NewServer(options...)
	nodeagentv1.RegisterNodeAgentServiceServer(server, service)

	return &Server{listener: listener, server: server}, nil
}

// Serve принимает запросы до остановки сервера или отказа listener.
func (s *Server) Serve() error {
	err := s.server.Serve(s.listener)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown ожидает завершения активных RPC. После истечения ctx Shutdown
// принудительно закрывает активные соединения и возвращает ошибку контекста.
func (s *Server) Shutdown(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		<-stopped
		return ctx.Err()
	}
}
