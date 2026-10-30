// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverboot

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/contextlogging"
)

// TestResolveLogsExporter pins the logs names on top of the shared rules: otlp
// and console, a default of none, and no other signal's names.
func TestResolveLogsExporter(t *testing.T) {
	ctx := context.Background()
	for value, want := range map[string]string{
		"":                "none",
		"otlp":            "otlp",
		"otlp,console":    "console,otlp",
		"console":         "console",
		"prometheus":      "none",
		"otlp,prometheus": "otlp",
	} {
		t.Setenv(logsExporterEnv, value)
		got := ResolveLogsExporter(ctx)
		// InitLogging rejects a nil set, so nil here would stop every component.
		if got == nil {
			t.Fatalf("ResolveLogsExporter() with %q = nil, want a set", value)
		}
		if got.String() != want {
			t.Errorf("ResolveLogsExporter() with %q = %q, want %q", value, got, want)
		}
	}
}

func TestInitLoggingDisabled(t *testing.T) {
	// global.SetLoggerProvider has no undo, so assert on the provider this
	// returns rather than on the global. A nil provider is what proves
	// InitLogging left the global alone.
	before := global.GetLoggerProvider()

	lp, err := InitLogging(context.Background(), LoggingOptions{
		ServiceName: "test",
		Exporter:    Exporters{},
	})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp != nil {
		t.Errorf("InitLogging() with the exporter disabled = %v, want nil", lp)
	}
	if got := global.GetLoggerProvider(); got != before {
		t.Errorf("InitLogging() with the exporter disabled replaced the global provider")
	}
}

// The log path's half of the relay decision, asserted on the resource an
// emitted record carries rather than on what relayAttrs returns, so dropping
// the attrs in newLoggerProvider fails here. TestRelayAttrs pins the values.
func TestLoggerProviderRelayAttribute(t *testing.T) {
	for _, tc := range []struct {
		name         string
		relayCapable bool
		conn         *grpc.ClientConn
		want         string // "" means absent
	}{
		{name: "relay", relayCapable: true, conn: lazyConn(t), want: "relay"},
		{name: "direct", relayCapable: true, want: "direct"},
		{name: "not relay capable", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := emittedResource(t, tc.relayCapable, tc.conn)[string(ateattr.OTLPRelayKey)]
			if ok != (tc.want != "") || got != tc.want {
				t.Errorf("%s = %q (present %t), want %q", string(ateattr.OTLPRelayKey), got, ok, tc.want)
			}
		})
	}
}

// emittedResource builds the provider the way InitLogging does, emits one
// record through a capturing processor, and returns that record's resource.
func emittedResource(t *testing.T, relayCapable bool, conn *grpc.ClientConn) map[string]string {
	t.Helper()
	proc := withCaptureProcessor(t)
	lp, err := newLoggerProvider(context.Background(), LoggingOptions{
		ServiceName:  "ateworker-gvisor",
		Exporter:     Exporters{ExporterOTLP: true},
		ExporterConn: conn,
		RelayCapable: relayCapable,
	})
	if err != nil {
		t.Fatalf("newLoggerProvider: %v", err)
	}
	t.Cleanup(func() {
		// A cancelled context skips the OTLP flush; no collector listens here.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = lp.Shutdown(ctx)
	})
	lp.Logger("test").Emit(context.Background(), otellog.Record{})
	if proc.res == nil {
		t.Fatal("no record reached the processor")
	}
	return resourceAttrs(proc.res)
}

type captureProcessor struct{ res *resource.Resource }

func (c *captureProcessor) OnEmit(_ context.Context, r *sdklog.Record) error {
	c.res = r.Resource()
	return nil
}
func (c *captureProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (c *captureProcessor) Shutdown(context.Context) error                         { return nil }
func (c *captureProcessor) ForceFlush(context.Context) error                       { return nil }

func TestInitLoggingRequiresOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts LoggingOptions
	}{
		{
			name: "no service name",
			opts: LoggingOptions{Exporter: Exporters{ExporterOTLP: true}},
		},
		{
			name: "no exporter",
			opts: LoggingOptions{ServiceName: "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := InitLogging(context.Background(), tt.opts); err == nil {
				t.Errorf("InitLogging(%+v) error = nil, want an error", tt.opts)
			}
		})
	}
}

// TestInitLoggingSetsConsole pins that InitLogging, which every component
// calls, decides whether an actor event also goes to stdout beside its OTLP
// event, so no binary has to set it on its own.
func TestInitLoggingSetsConsole(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	for _, tc := range []struct {
		exporter   Exporters
		wantStdout int
	}{
		{Exporters{ExporterConsole: true}, 1},
		{Exporters{}, 0},
	} {
		if _, err := InitLogging(context.Background(), LoggingOptions{ServiceName: "test", Exporter: tc.exporter}); err != nil {
			t.Fatalf("InitLogging(%v) error = %v", tc.exporter, err)
		}
		stdout := &countHandler{}
		lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(&captureProcessor{}))
		actorevent.NewEmitterTo(lp, stdout).Log(context.Background(), actorevent.StateChanged, nil)
		_ = lp.Shutdown(context.Background())
		if stdout.n != tc.wantStdout {
			t.Errorf("after InitLogging(%v), an emitted event wrote %d stdout records, want %d", tc.exporter, stdout.n, tc.wantStdout)
		}
	}
}

// TestInitLoggingOTLPAndConsole pins kind's setting: otlp,console installs the
// provider as the global, built from the caller's options, and also keeps each
// actor event on stdout.
func TestInitLoggingOTLPAndConsole(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	proc := withCaptureProcessor(t)
	lp, err := InitLogging(context.Background(), LoggingOptions{
		ServiceName:  "test",
		Exporter:     Exporters{ExporterOTLP: true, ExporterConsole: true},
		ExporterConn: lazyConn(t),
		RelayCapable: true,
	})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp == nil {
		t.Fatal("InitLogging() with otlp,console returned no provider")
	}
	if global.GetLoggerProvider() != lp {
		t.Error("InitLogging() with otlp,console did not install its provider as the global")
	}
	t.Cleanup(func() {
		// A cancelled context skips the OTLP flush; no collector listens here.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = lp.Shutdown(ctx)
	})
	stdout := &countHandler{}
	actorevent.NewEmitterTo(lp, stdout).Log(context.Background(), actorevent.StateChanged, nil)
	if stdout.n != 1 {
		t.Errorf("with otlp,console an emitted event wrote %d stdout records, want 1", stdout.n)
	}
	if proc.res == nil {
		t.Fatal("no record reached the provider")
	}
	attrs := resourceAttrs(proc.res)
	if got := attrs[string(ateattr.OTLPRelayKey)]; got != "relay" {
		t.Errorf("%s = %q, want relay: the caller's relay options did not reach the provider", ateattr.OTLPRelayKey, got)
	}
	if got := attrs["service.name"]; got != "test" {
		t.Errorf("service.name = %q, want test", got)
	}
}

// withCaptureProcessor adds a capture processor to the providers the test
// builds, until it ends.
func withCaptureProcessor(t *testing.T) *captureProcessor {
	t.Helper()
	proc := &captureProcessor{}
	testLogProcessors = []sdklog.Processor{proc}
	t.Cleanup(func() { testLogProcessors = nil })
	return proc
}

// countHandler counts the stdout records it is handed.
type countHandler struct{ n int }

func (h *countHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countHandler) Handle(context.Context, slog.Record) error { h.n++; return nil }
func (h *countHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countHandler) WithGroup(string) slog.Handler             { return h }

// TestInitLoggingConsoleAlone pins that console without otlp installs no
// provider: it only keeps the stdout record, which no provider means anyway.
func TestInitLoggingConsoleAlone(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	lp, err := InitLogging(context.Background(), LoggingOptions{ServiceName: "test", Exporter: Exporters{ExporterConsole: true}})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp != nil {
		t.Errorf("InitLogging() with console alone = %v, want nil", lp)
	}
}

// TestInitLoggerInstallsRedactingHandler pins the contract the gRPC
// interceptors rely on: the default logger every server gets from InitLogger
// masks debug_redact fields, so bodies can be logged as they are. Not parallel:
// it swaps the process-wide default logger.
func TestInitLoggerInstallsRedactingHandler(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	var buf bytes.Buffer
	InitLoggerWithWriter(&buf)

	if h := slog.Default().Handler(); h == nil {
		t.Fatal("no default handler installed")
	} else if _, ok := h.(*contextlogging.ContextHandler); !ok {
		t.Fatalf("default handler is %T, want *contextlogging.ContextHandler", h)
	}

	const token = "eyJhbGciOiJSUzI1NiJ9.secret-token"
	slog.Info("Handle RPC", slog.Any("resp", &ateapi.MintActorJwtResponse{ActorJwt: token}))
	got := buf.String()
	if strings.Contains(got, token) {
		t.Fatalf("default logger wrote a debug_redact field: %s", got)
	}
	if !strings.Contains(got, `"actor_jwt":"[REDACTED]"`) {
		t.Fatalf("default logger did not mask actor_jwt: %s", got)
	}
}

type eventNameCaptureProcessor struct {
	mu    sync.Mutex
	names []string
}

func (p *eventNameCaptureProcessor) OnEmit(_ context.Context, r *sdklog.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.names = append(p.names, r.EventName())
	return nil
}

func (p *eventNameCaptureProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool {
	return true
}
func (p *eventNameCaptureProcessor) Shutdown(context.Context) error   { return nil }
func (p *eventNameCaptureProcessor) ForceFlush(context.Context) error { return nil }

func TestLifecycleFilterProcessor(t *testing.T) {
	t.Parallel()

	sink := &eventNameCaptureProcessor{}
	filter := NewLifecycleFilterProcessor(sink)

	for _, tc := range []struct {
		eventName string
		want      bool
	}{
		{actorevent.StateChanged.Name, true},
		{actorevent.Crashed.Name, true},
		{actorevent.UsageSampled.Name, false},
		{"", false},
		{"ate.actor.other", false},
	} {
		if got := filter.Enabled(context.Background(), sdklog.EnabledParameters{EventName: tc.eventName}); got != tc.want {
			t.Errorf("Enabled(%q) = %v, want %v", tc.eventName, got, tc.want)
		}
	}

	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(filter))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })

	emitter := actorevent.NewEmitterTo(lp, &countHandler{})
	emitter.Log(context.Background(), actorevent.StateChanged, nil)
	emitter.Log(context.Background(), actorevent.UsageSampled, nil)
	emitter.Log(context.Background(), actorevent.Crashed, nil)

	var nonLifecycleRec otellog.Record
	nonLifecycleRec.SetEventName("custom.event")
	lp.Logger(actorevent.ScopeName).Emit(context.Background(), nonLifecycleRec)

	want := []string{actorevent.StateChanged.Name, actorevent.Crashed.Name}
	if !slices.Equal(sink.names, want) {
		t.Errorf("forwarded events = %v, want %v", sink.names, want)
	}
}

func TestParseLifecycleOTLPEndpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		raw            string
		wantHostPort   string
		wantServerName string
		wantErr        bool
	}{
		{
			name:           "host and port",
			raw:            "cost.ate-cost.svc:4317",
			wantHostPort:   "cost.ate-cost.svc:4317",
			wantServerName: "cost.ate-cost.svc",
		},
		{
			name:           "bare host defaults to 4317",
			raw:            "cost.ate-cost.svc",
			wantHostPort:   "cost.ate-cost.svc:4317",
			wantServerName: "cost.ate-cost.svc",
		},
		{
			name:           "https URL with port",
			raw:            "https://cost.ate-cost.svc:4317",
			wantHostPort:   "cost.ate-cost.svc:4317",
			wantServerName: "cost.ate-cost.svc",
		},
		{
			name:           "https URL without port defaults to 4317",
			raw:            "https://cost.ate-cost.svc",
			wantHostPort:   "cost.ate-cost.svc:4317",
			wantServerName: "cost.ate-cost.svc",
		},
		{
			name:           "ipv4 literal",
			raw:            "127.0.0.1:4317",
			wantHostPort:   "127.0.0.1:4317",
			wantServerName: "127.0.0.1",
		},
		{
			name:           "ipv6 literal with port",
			raw:            "[::1]:4317",
			wantHostPort:   "[::1]:4317",
			wantServerName: "::1",
		},
		{
			name:           "ipv6 literal without port",
			raw:            "[::1]",
			wantHostPort:   "[::1]:4317",
			wantServerName: "::1",
		},
		{
			name:    "empty",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "http scheme rejected",
			raw:     "http://cost.ate-cost.svc:4317",
			wantErr: true,
		},
		{
			name:    "missing host",
			raw:     ":4317",
			wantErr: true,
		},
		{
			name:    "invalid port zero",
			raw:     "cost.ate-cost.svc:0",
			wantErr: true,
		},
		{
			name:    "invalid port out of range",
			raw:     "cost.ate-cost.svc:70000",
			wantErr: true,
		},
		{
			name:    "invalid port non-numeric",
			raw:     "cost.ate-cost.svc:otlp",
			wantErr: true,
		},
		{
			name:    "url with path rejected",
			raw:     "https://cost.ate-cost.svc:4317/v1/logs",
			wantErr: true,
		},
		{
			name:    "hostport with path rejected",
			raw:     "cost.ate-cost.svc:4317/v1/logs",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotHostPort, gotServerName, err := ParseLifecycleOTLPEndpoint(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseLifecycleOTLPEndpoint(%q) error = %v, wantErr %v", tc.raw, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if gotHostPort != tc.wantHostPort || gotServerName != tc.wantServerName {
				t.Errorf("ParseLifecycleOTLPEndpoint(%q) = (%q, %q), want (%q, %q)",
					tc.raw, gotHostPort, gotServerName, tc.wantHostPort, tc.wantServerName)
			}
		})
	}
}

func TestBuildLifecycleOTLPTLSConfigValidation(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	writeTestFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}))

	clientBundle := filepath.Join(dir, "client-bundle.pem")
	writeTestFile(t, clientBundle, ca.issueClientBundle(t, "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"))

	for _, tc := range []struct {
		name string
		opts LifecycleOTLPOptions
	}{
		{
			name: "missing CAFile",
			opts: LifecycleOTLPOptions{ClientCredBundle: clientBundle},
		},
		{
			name: "missing ClientCredBundle",
			opts: LifecycleOTLPOptions{CAFile: caFile},
		},
		{
			name: "unreadable CAFile",
			opts: LifecycleOTLPOptions{
				CAFile:           filepath.Join(dir, "missing-ca.pem"),
				ClientCredBundle: clientBundle,
			},
		},
		{
			name: "unreadable ClientCredBundle",
			opts: LifecycleOTLPOptions{
				CAFile:           caFile,
				ClientCredBundle: filepath.Join(dir, "missing-client.pem"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := BuildLifecycleOTLPTLSConfig(tc.opts, "cost.ate-cost.svc"); err == nil {
				t.Errorf("BuildLifecycleOTLPTLSConfig(%+v) error = nil, want error", tc.opts)
			}
		})
	}
}

func TestInitLoggingLifecycleExporterOverMTLS(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })

	const expectedSPIFFEID = "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"
	const costDNSName = "cost.ate-cost.svc"

	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "trust-bundle.pem")
	writeTestFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}))

	clientBundle := filepath.Join(dir, "credential-bundle.pem")
	writeTestFile(t, clientBundle, ca.issueClientBundle(t, expectedSPIFFEID))

	serverCert := ca.issueServerCertWithDNS(t, costDNSName)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)

	collector := &testOTLPLogsServer{expectedURI: expectedSPIFFEID}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	})))
	collogspb.RegisterLogsServiceServer(srv, collector)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Default OTEL_LOGS_EXPORTER is none (Exporters{}): records must still be
	// written to stdout while lifecycle events are also exported to the Cost Service over
	// mTLS.
	lp, err := InitLogging(ctx, LoggingOptions{
		ServiceName: "ateapi",
		Exporter:    Exporters{},
		LifecycleOTLP: LifecycleOTLPOptions{
			Endpoint:         lis.Addr().String(),
			CAFile:           caFile,
			ClientCredBundle: clientBundle,
			ServerName:       costDNSName,
		},
	})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp == nil {
		t.Fatal("InitLogging() with LifecycleOTLP returned nil provider")
	}

	stdout := &countHandler{}
	emitter := actorevent.NewEmitterTo(lp, stdout)
	emitter.Log(ctx, actorevent.StateChanged, nil)
	emitter.Log(ctx, actorevent.UsageSampled, nil)
	emitter.Log(ctx, actorevent.Crashed, nil)

	if err := lp.Shutdown(ctx); err != nil {
		t.Fatalf("lp.Shutdown() error = %v", err)
	}

	// Default exporter is none, so all three events still wrote to stdout.
	if stdout.n != 3 {
		t.Errorf("stdout records = %d, want 3", stdout.n)
	}

	gotEvents := collector.eventNames()
	wantEvents := []string{actorevent.StateChanged.Name, actorevent.Crashed.Name}
	if !slices.Equal(gotEvents, wantEvents) {
		t.Errorf("mTLS lifecycle collector received %v, want %v", gotEvents, wantEvents)
	}

	// Now test with default OTEL_LOGS_EXPORTER=otlp enabled alongside
	// LifecycleOTLP: the default exporter receives all three events while the
	// lifecycle exporter receives only the two lifecycle events, and stdout is
	// not written.
	defaultCollector := &testOTLPLogsServer{}
	defaultSrv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(defaultSrv, defaultCollector)
	defaultLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen default: %v", err)
	}
	go defaultSrv.Serve(defaultLis)
	defer defaultSrv.Stop()

	defaultConn, err := grpc.NewClient(defaultLis.Addr().String(), grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}), //nolint:gosec
	))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer defaultConn.Close()
	// Use insecure credentials for the plaintext default collector.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+defaultLis.Addr().String())

	collector.mu.Lock()
	collector.events = nil
	collector.mu.Unlock()

	lpBoth, err := InitLogging(ctx, LoggingOptions{
		ServiceName: "ateapi",
		Exporter:    Exporters{ExporterOTLP: true},
		LifecycleOTLP: LifecycleOTLPOptions{
			Endpoint:         "https://" + lis.Addr().String(),
			CAFile:           caFile,
			ClientCredBundle: clientBundle,
			ServerName:       costDNSName,
		},
	})
	if err != nil {
		t.Fatalf("InitLogging(both) error = %v", err)
	}

	stdoutBoth := &countHandler{}
	emitterBoth := actorevent.NewEmitterTo(lpBoth, stdoutBoth)
	emitterBoth.Log(ctx, actorevent.StateChanged, nil)
	emitterBoth.Log(ctx, actorevent.UsageSampled, nil)
	emitterBoth.Log(ctx, actorevent.Crashed, nil)

	if err := lpBoth.Shutdown(ctx); err != nil {
		t.Fatalf("lpBoth.Shutdown() error = %v", err)
	}
	if stdoutBoth.n != 0 {
		t.Errorf("with ExporterOTLP only, stdout records = %d, want 0", stdoutBoth.n)
	}
	wantAll := []string{actorevent.StateChanged.Name, actorevent.UsageSampled.Name, actorevent.Crashed.Name}
	if got := defaultCollector.eventNames(); !slices.Equal(got, wantAll) {
		t.Errorf("default collector received %v, want %v", got, wantAll)
	}
	if got := collector.eventNames(); !slices.Equal(got, wantEvents) {
		t.Errorf("lifecycle collector received %v, want %v", got, wantEvents)
	}
}

func TestBuildLifecycleOTLPTLSConfigReloadsCAFile(t *testing.T) {
	t.Parallel()

	const costDNSName = "cost.ate-cost.svc"
	ca1 := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "trust-bundle.pem")
	writeTestFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca1.certDER}))

	clientBundle := filepath.Join(dir, "credential-bundle.pem")
	writeTestFile(t, clientBundle, ca1.issueClientBundle(t, "spiffe://cluster.local/ns/ate-system/sa/ate-api-server"))

	tlsCfg, err := BuildLifecycleOTLPTLSConfig(LifecycleOTLPOptions{
		CAFile:           caFile,
		ClientCredBundle: clientBundle,
		ServerName:       costDNSName,
	}, costDNSName)
	if err != nil {
		t.Fatalf("BuildLifecycleOTLPTLSConfig() error = %v", err)
	}

	ca2 := newTestCA(t)
	serverCert2 := ca2.issueServerCertWithDNS(t, costDNSName)
	leaf2, err := x509.ParseCertificate(serverCert2.Certificate[0])
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}

	// Before rotating caFile on disk, a peer certificate signed by ca2 must fail verification.
	if err := tlsCfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf2}}); err == nil {
		t.Fatal("VerifyConnection() succeeded for ca2 before CA file rotation, want error")
	}

	// Rotate caFile on disk to ca2; VerifyConnection must pick up the new trust bundle without rebuilding tlsCfg.
	writeTestFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca2.certDER}))
	if err := tlsCfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf2}}); err != nil {
		t.Fatalf("VerifyConnection() after CA file rotation error = %v, want nil", err)
	}
}

type testOTLPLogsServer struct {
	collogspb.UnimplementedLogsServiceServer
	expectedURI string

	mu     sync.Mutex
	events []string
}

func (s *testOTLPLogsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if s.expectedURI != "" {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing peer")
		}
		tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing client certificate")
		}
		var matched bool
		for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
			if uri.String() == s.expectedURI {
				matched = true
				break
			}
		}
		if !matched {
			return nil, status.Errorf(codes.PermissionDenied, "client cert URIs %v do not match %q", tlsInfo.State.PeerCertificates[0].URIs, s.expectedURI)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				s.events = append(s.events, lr.GetEventName())
			}
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s *testOTLPLogsServer) eventNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.events)
}

type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{cert: cert, certDER: der, key: key}
}

func (ca *testCA) issueClientBundle(t *testing.T, spiffeID string) []byte {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

func (ca *testCA) issueServerCertWithDNS(t *testing.T, dnsName string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{dnsName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
