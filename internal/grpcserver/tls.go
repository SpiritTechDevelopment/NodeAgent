package grpcserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
)

// TLSConfig описывает сертификат сервера, хранилище доверенных клиентских CA
// и разрешённые идентичности backend.
type TLSConfig struct {
	// CertificateFile задаёт путь к PEM-файлу с цепочкой сертификатов сервера.
	CertificateFile string
	// PrivateKeyFile задаёт путь к PEM-файлу с закрытым ключом сервера.
	PrivateKeyFile string
	// ClientCAFile задаёт путь к PEM-файлу с CA для проверки клиентов.
	ClientCAFile string
	// AllowedClientIdentities содержит точные значения DNS SAN или URI SAN.
	AllowedClientIdentities []string
}

// Load проверяет конфигурацию и возвращает настройки TLS, которые требуют
// проверенный клиентский сертификат с разрешённой SAN-идентичностью.
func (c TLSConfig) Load() (*tls.Config, error) {
	identities, err := c.validate()
	if err != nil {
		return nil, err
	}

	certificate, err := tls.LoadX509KeyPair(c.CertificateFile, c.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}

	clientCAPEM, err := os.ReadFile(c.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA bundle: %w", err)
	}

	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, errors.New("client CA bundle contains no certificates")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		VerifyConnection: func(state tls.ConnectionState) error {
			return verifyClientIdentity(state, identities)
		},
	}, nil
}

func (c TLSConfig) validate() ([]string, error) {
	switch {
	case strings.TrimSpace(c.CertificateFile) == "":
		return nil, errors.New("server certificate file is required")
	case strings.TrimSpace(c.PrivateKeyFile) == "":
		return nil, errors.New("server private key file is required")
	case strings.TrimSpace(c.ClientCAFile) == "":
		return nil, errors.New("client CA file is required")
	case len(c.AllowedClientIdentities) == 0:
		return nil, errors.New("at least one client identity is required")
	}

	identities := make([]string, 0, len(c.AllowedClientIdentities))
	seen := make(map[string]struct{}, len(c.AllowedClientIdentities))
	for _, raw := range c.AllowedClientIdentities {
		identity := strings.TrimSpace(raw)
		if identity == "" {
			return nil, errors.New("client identity must not be empty")
		}
		if _, exists := seen[identity]; exists {
			return nil, fmt.Errorf("duplicate client identity %q", identity)
		}
		seen[identity] = struct{}{}
		identities = append(identities, identity)
	}

	slices.Sort(identities)
	return identities, nil
}

func verifyClientIdentity(state tls.ConnectionState, allowed []string) error {
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return errors.New("client certificate has no verified chain")
	}

	leaf := state.VerifiedChains[0][0]
	for _, dnsName := range leaf.DNSNames {
		if slices.Contains(allowed, dnsName) {
			return nil
		}
	}
	for _, uri := range leaf.URIs {
		if slices.Contains(allowed, uri.String()) {
			return nil
		}
	}

	return errors.New("client certificate has no allowed SAN identity")
}
