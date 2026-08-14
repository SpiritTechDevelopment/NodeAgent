package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
)

const (
	testServerName      = "agent.test"
	testClientIdentity  = "backend.test"
	testClientSPIFFEURI = "spiffe://spiritvpn/backend"
)

type healthService struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer
}

func (healthService) Health(context.Context, *nodeagentv1.HealthRequest) (*nodeagentv1.HealthResponse, error) {
	return &nodeagentv1.HealthResponse{NodeId: "node-test", AgentVersion: "test"}, nil
}

func TestServerRequiresAllowedClientIdentity(t *testing.T) {
	pki := newTestPKI(t)
	serverCertificate := pki.issue(t, certificateSpec{
		commonName: testServerName,
		dnsNames:   []string{testServerName},
		usage:      x509.ExtKeyUsageServerAuth,
	})
	allowedClient := pki.issue(t, certificateSpec{
		commonName: "ignored-client-cn",
		dnsNames:   []string{testClientIdentity},
		usage:      x509.ExtKeyUsageClientAuth,
	})
	wrongClient := pki.issue(t, certificateSpec{
		commonName: "ignored-client-cn",
		dnsNames:   []string{"other-backend.test"},
		usage:      x509.ExtKeyUsageClientAuth,
	})
	roguePKI := newTestPKI(t)
	untrustedClient := roguePKI.issue(t, certificateSpec{
		commonName: "ignored-client-cn",
		dnsNames:   []string{testClientIdentity},
		usage:      x509.ExtKeyUsageClientAuth,
	})

	address, stop := startTestServer(t, pki, serverCertificate, []string{testClientIdentity})
	defer stop()

	tests := []struct {
		name        string
		certificate *tls.Certificate
		maxVersion  uint16
		wantCode    codes.Code
	}{
		{name: "allowed identity", certificate: &allowedClient.tlsCertificate, wantCode: codes.OK},
		{name: "wrong identity", certificate: &wrongClient.tlsCertificate, wantCode: codes.Unavailable},
		{name: "untrusted certificate", certificate: &untrustedClient.tlsCertificate, wantCode: codes.Unavailable},
		{name: "missing certificate", wantCode: codes.Unavailable},
		{
			name:        "TLS 1.2",
			certificate: &allowedClient.tlsCertificate,
			maxVersion:  tls.VersionTLS12,
			wantCode:    codes.Unavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientTLS := &tls.Config{
				MinVersion: tls.VersionTLS13,
				RootCAs:    pki.roots,
				ServerName: testServerName,
			}
			if test.maxVersion != 0 {
				clientTLS.MinVersion = tls.VersionTLS12
				clientTLS.MaxVersion = test.maxVersion
			}
			if test.certificate != nil {
				clientTLS.Certificates = []tls.Certificate{*test.certificate}
			}

			connection, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
			)
			if err != nil {
				t.Fatalf("create gRPC client: %v", err)
			}
			defer connection.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			response, err := nodeagentv1.NewNodeAgentServiceClient(connection).Health(ctx, &nodeagentv1.HealthRequest{})
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("Health code = %s, want %s; error: %v", got, test.wantCode, err)
			}
			if test.wantCode == codes.OK && response.GetNodeId() != "node-test" {
				t.Fatalf("Health node_id = %q, want node-test", response.GetNodeId())
			}
		})
	}
}

func TestVerifyClientIdentityAcceptsURISANAndIgnoresCommonName(t *testing.T) {
	spiffeURI, err := url.Parse(testClientSPIFFEURI)
	if err != nil {
		t.Fatalf("parse SPIFFE URI: %v", err)
	}

	tests := []struct {
		name        string
		certificate *x509.Certificate
		allowed     []string
		wantError   bool
	}{
		{
			name:        "URI SAN",
			certificate: &x509.Certificate{URIs: []*url.URL{spiffeURI}},
			allowed:     []string{testClientSPIFFEURI},
		},
		{
			name:        "DNS SAN",
			certificate: &x509.Certificate{DNSNames: []string{testClientIdentity}},
			allowed:     []string{testClientIdentity},
		},
		{
			name:        "common name only",
			certificate: &x509.Certificate{Subject: pkix.Name{CommonName: testClientIdentity}},
			allowed:     []string{testClientIdentity},
			wantError:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{test.certificate}}}
			err := verifyClientIdentity(state, test.allowed)
			if test.wantError && err == nil {
				t.Fatal("verifyClientIdentity returned nil, want error")
			}
			if !test.wantError && err != nil {
				t.Fatalf("verifyClientIdentity returned error: %v", err)
			}
		})
	}
}

func TestTLSConfigRejectsIncompleteConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config TLSConfig
		want   string
	}{
		{name: "missing certificate", config: TLSConfig{}, want: "certificate"},
		{name: "missing private key", config: TLSConfig{CertificateFile: "cert.pem"}, want: "private key"},
		{
			name: "missing CA",
			config: TLSConfig{
				CertificateFile: "cert.pem",
				PrivateKeyFile:  "key.pem",
			},
			want: "client CA",
		},
		{
			name: "missing identity",
			config: TLSConfig{
				CertificateFile: "cert.pem",
				PrivateKeyFile:  "key.pem",
				ClientCAFile:    "ca.pem",
			},
			want: "client identity",
		},
		{
			name: "duplicate identity",
			config: TLSConfig{
				CertificateFile:         "cert.pem",
				PrivateKeyFile:          "key.pem",
				ClientCAFile:            "ca.pem",
				AllowedClientIdentities: []string{testClientIdentity, testClientIdentity},
			},
			want: "duplicate",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.config.Load()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error = %v, want text %q", err, test.want)
			}
		})
	}
}

type testPKI struct {
	certificate    *x509.Certificate
	privateKey     *ecdsa.PrivateKey
	certificatePEM []byte
	roots          *x509.CertPool
	directory      string
}

type certificateSpec struct {
	commonName string
	dnsNames   []string
	usage      x509.ExtKeyUsage
}

type issuedCertificate struct {
	certificateFile string
	privateKeyFile  string
	tlsCertificate  tls.Certificate
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "node-agent-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("append test CA certificate")
	}

	return &testPKI{
		certificate:    certificate,
		privateKey:     privateKey,
		certificatePEM: certificatePEM,
		roots:          roots,
		directory:      t.TempDir(),
	}
}

func (pki *testPKI) issue(t *testing.T, spec certificateSpec) issuedCertificate {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate certificate key: %v", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate certificate serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: spec.commonName},
		DNSNames:     spec.dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{spec.usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, pki.certificate, &privateKey.PublicKey, pki.privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal certificate key: %v", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER})
	tlsCertificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	if err != nil {
		t.Fatalf("parse issued key pair: %v", err)
	}

	baseName := strings.ReplaceAll(spec.commonName, ":", "-")
	certificateFile := filepath.Join(pki.directory, baseName+".crt")
	privateKeyFile := filepath.Join(pki.directory, baseName+".key")
	writeTestFile(t, certificateFile, certificatePEM, 0o644)
	writeTestFile(t, privateKeyFile, privateKeyPEM, 0o600)

	return issuedCertificate{
		certificateFile: certificateFile,
		privateKeyFile:  privateKeyFile,
		tlsCertificate:  tlsCertificate,
	}
}

func (pki *testPKI) writeCA(t *testing.T) string {
	t.Helper()

	path := filepath.Join(pki.directory, "ca.crt")
	writeTestFile(t, path, pki.certificatePEM, 0o644)
	return path
}

func writeTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatalf("write %s: %v", filepath.Base(path), err)
	}
}

func startTestServer(
	t *testing.T,
	pki *testPKI,
	serverCertificate issuedCertificate,
	allowedIdentities []string,
) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server, err := New(listener, Config{
		TLS: TLSConfig{
			CertificateFile:         serverCertificate.certificateFile,
			PrivateKeyFile:          serverCertificate.privateKeyFile,
			ClientCAFile:            pki.writeCA(t),
			AllowedClientIdentities: allowedIdentities,
		},
	}, healthService{})
	if err != nil {
		listener.Close()
		t.Fatalf("create gRPC server: %v", err)
	}

	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve()
	}()

	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shut down gRPC server: %v", err)
		}
		if err := <-serveResult; err != nil {
			t.Errorf("serve gRPC: %v", err)
		}
	}
	return listener.Addr().String(), stop
}
