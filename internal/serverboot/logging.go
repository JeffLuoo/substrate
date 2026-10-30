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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/credbundle"
)

const (
	logsExporterEnv     = "OTEL_LOGS_EXPORTER"
	defaultOTLPGRPCPort = "4317"
)

// logsExporters are the names OTEL_LOGS_EXPORTER accepts. console is the
// actor events' stdout form, the component's own JSON log line.
var logsExporters = []string{ExporterOTLP, ExporterConsole}

// ResolveLogsExporter reads OTEL_LOGS_EXPORTER with the shared exporter rules.
// The default is none rather than the spec's otlp, so turning it on is one
// ConfigMap key per environment.
func ResolveLogsExporter(ctx context.Context) Exporters {
	return resolveExportersEnv(ctx, logsExporterEnv, logsExporters, Exporters{})
}

// LifecycleOTLPOptions configures an opt-in second OTLP/gRPC log exporter that
// receives only actor lifecycle events (ate.actor.state_changed and
// ate.actor.crashed) over mutual TLS. An empty Endpoint (and nil ExporterConn)
// disables it.
type LifecycleOTLPOptions struct {
	// Endpoint is the target OTLP/gRPC endpoint (host:port, host defaulting to
	// port 4317, or https://host[:port]). Empty disables the exporter.
	Endpoint string
	// CAFile is the PEM trust bundle used to verify the receiver's serving
	// certificate. Required when Endpoint is set and ExporterConn is nil.
	CAFile string
	// ClientCredBundle is the PEM credential bundle (PKCS#8 private key +
	// certificate chain) presented as the client certificate. Required when
	// Endpoint is set and ExporterConn is nil.
	ClientCredBundle string
	// ServerName overrides the TLS SNI / DNS name verified against the server
	// certificate. Defaults to the host parsed from Endpoint.
	ServerName string
	// ExporterConn overrides the gRPC connection used by the exporter (for
	// tests).
	ExporterConn *grpc.ClientConn
}

func (o LifecycleOTLPOptions) enabled() bool {
	return strings.TrimSpace(o.Endpoint) != "" || o.ExporterConn != nil
}

// LoggingOptions configures InitLogging.
type LoggingOptions struct {
	// ServiceName is required; populates resource.semconv ServiceName.
	ServiceName string
	// Exporter is required; the empty set is none. Build it with
	// ResolveLogsExporter.
	Exporter Exporters
	// ExporterConn and RelayCapable are the logs counterpart of the same fields
	// on TracingOptions: worker passes its relay connection and marks itself
	// relay-capable so the resource carries ate.otlp.relay.
	ExporterConn *grpc.ClientConn
	RelayCapable bool
	// LifecycleOTLP optionally configures a second, filtered OTLP/gRPC log
	// exporter that receives only ate.actor.state_changed and ate.actor.crashed
	// records over mTLS.
	LifecycleOTLP LifecycleOTLPOptions
}

// InitLogging registers a global LoggerProvider for the records components emit
// through internal/actorevent, and sets whether those records also go to stdout
// (console).
//
// It returns (nil, nil) when neither the default OTLP exporter nor the
// lifecycle OTLP exporter is enabled, and leaves the OTel global alone:
// emitting then costs one Enabled check. The default exporter is off when the
// exporter does not include otlp, or when a relay-capable component has
// neither a relay connection nor an OTLP endpoint (see hasOTLPDestination).
// Guard the shutdown on a nil provider, unlike InitTracing which always
// returns one.
//
// The processor batches. These records sit on the actor resume and suspend
// path, and exporting inside the emit call would put a blocking gRPC round trip
// there, serialized across every emitting goroutine, so an unreachable
// collector would surface as control-plane latency. The cost is that an
// ungraceful exit loses the records still queued, unless console also wrote
// them to stdout.
func InitLogging(ctx context.Context, opts LoggingOptions) (*sdklog.LoggerProvider, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("LoggingOptions.ServiceName is required")
	}
	if opts.Exporter == nil {
		return nil, fmt.Errorf("LoggingOptions.Exporter is required")
	}
	defaultReason, defaultLevel := otlpExportBlocked(opts.Exporter.Has(ExporterOTLP),
		hasOTLPDestination(logsEndpointEnv, opts.RelayCapable, opts.ExporterConn))
	hasDefaultOTLP := defaultReason == ""
	hasLifecycle := opts.LifecycleOTLP.enabled()
	// When the default OTLP exporter is off, actor events belong on stdout
	// even if the extra lifecycle OTLP exporter installs a provider.
	actorevent.SetConsole(opts.Exporter.Has(ExporterConsole) || (hasLifecycle && !hasDefaultOTLP))
	if !hasDefaultOTLP && !hasLifecycle {
		slog.Log(ctx, defaultLevel, "OTLP logs export disabled",
			slog.String("exporter", opts.Exporter.String()),
			slog.String("reason", defaultReason))
		return nil, nil
	}

	lp, err := newLoggerProvider(ctx, opts)
	if err != nil {
		return nil, err
	}
	global.SetLoggerProvider(lp)
	slog.InfoContext(ctx, "Logging initialized",
		slog.String("exporter", opts.Exporter.String()),
		slog.Bool("lifecycle-otlp-exporter", hasLifecycle),
	)
	return lp, nil
}

// testLogProcessors are added to every provider newLoggerProvider builds, so a
// test can read the records, and their resource, that the provider emits.
var testLogProcessors []sdklog.Processor

// newLoggerProvider builds the provider InitLogging installs.
func newLoggerProvider(ctx context.Context, opts LoggingOptions) (*sdklog.LoggerProvider, error) {
	res, err := newResource(ctx, opts.ServiceName, relayAttrs(opts.RelayCapable, opts.ExporterConn)...)
	if err != nil {
		return nil, fmt.Errorf("create logger resource: %w", err)
	}

	popts := []sdklog.LoggerProviderOption{
		sdklog.WithResource(res),
	}

	if opts.Exporter.Has(ExporterOTLP) && hasOTLPDestination(logsEndpointEnv, opts.RelayCapable, opts.ExporterConn) {
		// Matches the trace and metric exporters: GKE managed telemetry does not
		// support validating the TLS certs of the collector.
		expOpts := []otlploggrpc.Option{otlploggrpc.WithInsecure()}
		if opts.ExporterConn != nil {
			// WithGRPCConn takes precedence over endpoint/credential options, so
			// WithInsecure above is inert on this path.
			expOpts = append(expOpts, otlploggrpc.WithGRPCConn(opts.ExporterConn))
		}
		exporter, err := otlploggrpc.New(ctx, expOpts...)
		if err != nil {
			return nil, fmt.Errorf("create OTLP log exporter: %w", err)
		}
		popts = append(popts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)))
	}

	if opts.LifecycleOTLP.enabled() {
		lifecycleExp, err := newLifecycleOTLPExporter(ctx, opts.LifecycleOTLP)
		if err != nil {
			return nil, err
		}
		popts = append(popts, sdklog.WithProcessor(NewLifecycleFilterProcessor(sdklog.NewBatchProcessor(lifecycleExp))))
	}

	for _, p := range testLogProcessors {
		popts = append(popts, sdklog.WithProcessor(p))
	}
	return sdklog.NewLoggerProvider(popts...), nil
}

func newLifecycleOTLPExporter(ctx context.Context, opts LifecycleOTLPOptions) (sdklog.Exporter, error) {
	var expOpts []otlploggrpc.Option
	if opts.ExporterConn != nil {
		expOpts = append(expOpts, otlploggrpc.WithGRPCConn(opts.ExporterConn))
	} else {
		hostPort, serverName, err := ParseLifecycleOTLPEndpoint(opts.Endpoint)
		if err != nil {
			return nil, err
		}
		tlsCfg, err := BuildLifecycleOTLPTLSConfig(opts, serverName)
		if err != nil {
			return nil, err
		}
		expOpts = append(expOpts,
			otlploggrpc.WithEndpoint(hostPort),
			otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)),
		)
	}
	exporter, err := otlploggrpc.New(ctx, expOpts...)
	if err != nil {
		return nil, fmt.Errorf("create lifecycle OTLP log exporter: %w", err)
	}
	return exporter, nil
}

// ParseLifecycleOTLPEndpoint parses an OTLP/gRPC endpoint string (host:port,
// host, or https://host[:port]) into a host:port dial target and default TLS
// server name. Plaintext http:// endpoints are rejected because the lifecycle
// exporter requires mTLS.
func ParseLifecycleOTLPEndpoint(raw string) (hostPort, serverName string, err error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", errors.New("lifecycle OTLP endpoint is empty")
	}
	target := s
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", "", fmt.Errorf("parse lifecycle OTLP endpoint %q: %w", raw, err)
		}
		if u.Scheme != "https" {
			return "", "", fmt.Errorf("lifecycle OTLP endpoint %q must use https or host:port, got scheme %q", raw, u.Scheme)
		}
		if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return "", "", fmt.Errorf("lifecycle OTLP endpoint %q must not contain userinfo, path, query, or fragment", raw)
		}
		target = u.Host
	} else if strings.ContainsAny(s, "/?#") {
		return "", "", fmt.Errorf("lifecycle OTLP endpoint %q must not contain path, query, or fragment", raw)
	}

	host, port, splitErr := net.SplitHostPort(target)
	if splitErr != nil {
		trimmed := strings.TrimPrefix(strings.TrimSuffix(target, "]"), "[")
		if trimmed == "" || strings.Contains(trimmed, "]") || strings.Contains(trimmed, "[") {
			return "", "", fmt.Errorf("invalid lifecycle OTLP endpoint %q: %w", raw, splitErr)
		}
		if strings.Contains(trimmed, ":") && net.ParseIP(trimmed) == nil {
			return "", "", fmt.Errorf("invalid lifecycle OTLP endpoint %q: %w", raw, splitErr)
		}
		host = trimmed
		port = defaultOTLPGRPCPort
	}
	if host == "" {
		return "", "", fmt.Errorf("lifecycle OTLP endpoint %q is missing a host", raw)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return "", "", fmt.Errorf("lifecycle OTLP endpoint %q has invalid port %q", raw, port)
	}
	return net.JoinHostPort(host, port), host, nil
}

// BuildLifecycleOTLPTLSConfig constructs the client mTLS configuration for the
// lifecycle OTLP exporter. Both the server CA trust bundle and the client
// credential bundle are validated at startup and reloaded on disk change on
// subsequent handshakes.
func BuildLifecycleOTLPTLSConfig(opts LifecycleOTLPOptions, defaultServerName string) (*tls.Config, error) {
	if opts.CAFile == "" {
		return nil, errors.New("lifecycle OTLP exporter requires --lifecycle-events-otlp-ca-file")
	}
	if opts.ClientCredBundle == "" {
		return nil, errors.New("lifecycle OTLP exporter requires --lifecycle-events-otlp-client-cred-bundle")
	}
	loadRootCAs := credbundle.PoolLoader(opts.CAFile)
	if _, err := loadRootCAs(); err != nil {
		return nil, fmt.Errorf("load lifecycle OTLP CA file: %w", err)
	}
	loadClientCert := credbundle.ClientLoader(opts.ClientCredBundle)
	if _, err := loadClientCert(nil); err != nil {
		return nil, fmt.Errorf("load lifecycle OTLP client credential bundle: %w", err)
	}
	serverName := opts.ServerName
	if serverName == "" {
		serverName = defaultServerName
	}
	tlsCfg := &tls.Config{
		MinVersion:           tls.VersionTLS13,
		ServerName:           serverName,
		GetClientCertificate: loadClientCert,
		// Chain and hostname verification run in VerifyConnection against the
		// reloadable CA pool so trust-bundle rotations apply without a restart.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(cs tls.ConnectionState) error {
			pool, err := loadRootCAs()
			if err != nil {
				return err
			}
			if len(cs.PeerCertificates) == 0 {
				return errors.New("lifecycle OTLP server presented no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			dnsName := serverName
			if dnsName == "" {
				dnsName = cs.ServerName
			}
			_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         pool,
				Intermediates: inter,
				DNSName:       dnsName,
			})
			return err
		},
	}
	return tlsCfg, nil
}

// lifecycleFilterProcessor wraps a sdklog.Processor and forwards only actor
// lifecycle events (ate.actor.state_changed and ate.actor.crashed).
type lifecycleFilterProcessor struct {
	next sdklog.Processor
}

// NewLifecycleFilterProcessor returns a sdklog.Processor that forwards only
// actor lifecycle records (ate.actor.state_changed and ate.actor.crashed) to
// next.
func NewLifecycleFilterProcessor(next sdklog.Processor) sdklog.Processor {
	return &lifecycleFilterProcessor{next: next}
}

func (p *lifecycleFilterProcessor) OnEmit(ctx context.Context, r *sdklog.Record) error {
	if !actorevent.IsLifecycleEvent(r.EventName()) {
		return nil
	}
	return p.next.OnEmit(ctx, r)
}

func (p *lifecycleFilterProcessor) Enabled(ctx context.Context, params sdklog.EnabledParameters) bool {
	if !actorevent.IsLifecycleEvent(params.EventName) {
		return false
	}
	return p.next.Enabled(ctx, params)
}

func (p *lifecycleFilterProcessor) Shutdown(ctx context.Context) error {
	return p.next.Shutdown(ctx)
}

func (p *lifecycleFilterProcessor) ForceFlush(ctx context.Context) error {
	return p.next.ForceFlush(ctx)
}
