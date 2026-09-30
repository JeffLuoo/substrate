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
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestResolveMetricsExporter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		isSet   bool
		want    metricsExporter
		wantErr bool
	}{
		{name: "unset is otlp", want: metricsExporterOTLP},
		{name: "set but empty reads as unset", value: "", isSet: true, want: metricsExporterOTLP},
		{name: "whitespace only reads as unset", value: "  ", isSet: true, want: metricsExporterOTLP},
		{name: "otlp", value: "otlp", isSet: true, want: metricsExporterOTLP},
		{name: "none", value: "none", isSet: true, want: metricsExporterNone},
		{name: "mixed case and padding", value: " None\t", isSet: true, want: metricsExporterNone},
		{name: "unsupported value keeps otlp", value: "prometheus", isSet: true, want: metricsExporterOTLP, wantErr: true},
		{name: "a list keeps otlp", value: "none,otlp", isSet: true, want: metricsExporterOTLP, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveMetricsExporter(tt.value, tt.isSet)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveMetricsExporter(%q, %v) error = %v, wantErr %v", tt.value, tt.isSet, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("resolveMetricsExporter(%q, %v) = %q, want %q", tt.value, tt.isSet, got, tt.want)
			}
		})
	}
}

func TestMetricsExporterFromEnv(t *testing.T) {
	ctx := context.Background()

	t.Setenv(metricsExporterEnv, "none")
	if got := metricsExporterFromEnv(ctx); got != metricsExporterNone {
		t.Errorf("metricsExporterFromEnv() = %q, want %q", got, metricsExporterNone)
	}

	t.Setenv(metricsExporterEnv, "not_an_exporter")
	if got := metricsExporterFromEnv(ctx); got != metricsExporterOTLP {
		t.Errorf("metricsExporterFromEnv() on an invalid value = %q, want %q", got, metricsExporterOTLP)
	}
}

// fakeCollector points OTEL_EXPORTER_OTLP_ENDPOINT at a local listener and
// returns a function that reports whether anything connected to it. The
// listener closes each connection, so an export fails; the tests need only the
// dial.
func fakeCollector(t *testing.T) func() bool {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { lis.Close() })
	var dialed atomic.Bool
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			dialed.Store(true)
			conn.Close()
		}
	}()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+lis.Addr().String())
	return dialed.Load
}

// flushAndShutdown forces one export attempt, then shuts the provider down.
// The deadline bounds the exporter's retries against the fake collector.
func flushAndShutdown(t *testing.T, mp *sdkmetric.MeterProvider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = mp.ForceFlush(ctx)
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	_ = mp.Shutdown(cancelled)
}

// recordOne records a counter so that an export has data to send.
func recordOne(t *testing.T, mp *sdkmetric.MeterProvider, name string) {
	t.Helper()
	ctr, err := mp.Meter("test").Int64Counter(name)
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}
	ctr.Add(context.Background(), 1)
}

// The positive control for the tests below: without the variable, the OTLP
// reader dials the collector. Without it, a "does not dial" assertion could
// pass because the fake collector never works.
func TestInitMetricsPushOnlyDialsByDefault(t *testing.T) {
	dialed := fakeCollector(t)
	mp, err := InitMetricsPushOnly(context.Background(), "test-default")
	if err != nil {
		t.Fatalf("InitMetricsPushOnly: %v", err)
	}
	recordOne(t, mp, "ate.test.default.count")
	flushAndShutdown(t, mp)
	if !dialed() {
		t.Error("the OTLP reader did not dial the collector with OTEL_METRICS_EXPORTER unset")
	}
}

func TestMetricsExporterNoneDoesNotDial(t *testing.T) {
	for name, initFn := range map[string]func(ctx context.Context) (*sdkmetric.MeterProvider, error){
		"InitMetricsPushOnly": func(ctx context.Context) (*sdkmetric.MeterProvider, error) {
			return InitMetricsPushOnly(ctx, "test-none")
		},
		"InitMetricsPushOnlyVia": func(ctx context.Context) (*sdkmetric.MeterProvider, error) {
			return InitMetricsPushOnlyVia(ctx, "test-none", nil)
		},
		"InitMetricsBridged": func(ctx context.Context) (*sdkmetric.MeterProvider, error) {
			return InitMetricsBridged(ctx, "test-none", prometheus.NewRegistry())
		},
	} {
		t.Run(name, func(t *testing.T) {
			dialed := fakeCollector(t)
			t.Setenv(metricsExporterEnv, "none")
			mp, err := initFn(context.Background())
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			recordOne(t, mp, "ate.test.none.count")
			flushAndShutdown(t, mp)
			if dialed() {
				t.Errorf("%s dialed the collector with OTEL_METRICS_EXPORTER=none", name)
			}
		})
	}
}

// gatheredNames returns the metric family names in reg.
func gatheredNames(t *testing.T, reg prometheus.Gatherer) string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return strings.Join(names, " ")
}

// With none, atecontroller's instruments must move onto the registry its
// manager serves, or turning off the push would lose them.
func TestInitMetricsBridgedNoneServesOnRegistry(t *testing.T) {
	t.Setenv(metricsExporterEnv, "none")
	reg := prometheus.NewRegistry()
	mp, err := InitMetricsBridged(context.Background(), "test-bridged", reg)
	if err != nil {
		t.Fatalf("InitMetricsBridged: %v", err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	recordOne(t, mp, "ate.test.bridged.count")

	if got := gatheredNames(t, reg); !strings.Contains(got, "ate_test_bridged_count") {
		t.Errorf("registry families = %q, want ate_test_bridged_count", got)
	}
}

// With otlp, the registry is bridged onto the push path. The OTel instruments
// must stay off the registry, or the bridge would push them a second time.
func TestInitMetricsBridgedOTLPPushesOnly(t *testing.T) {
	dialed := fakeCollector(t)
	reg := prometheus.NewRegistry()
	mp, err := InitMetricsBridged(context.Background(), "test-bridged", reg)
	if err != nil {
		t.Fatalf("InitMetricsBridged: %v", err)
	}
	recordOne(t, mp, "ate.test.bridged.count")

	if got := gatheredNames(t, reg); strings.Contains(got, "ate_test_bridged") {
		t.Errorf("registry families = %q, want no ate_test_bridged family", got)
	}
	flushAndShutdown(t, mp)
	if !dialed() {
		t.Error("InitMetricsBridged did not dial the collector with OTEL_METRICS_EXPORTER unset")
	}
}

func TestInitMetricsBridgedRequiresServiceName(t *testing.T) {
	if _, err := InitMetricsBridged(context.Background(), "", prometheus.NewRegistry()); err == nil {
		t.Error("InitMetricsBridged(\"\") must return an error")
	}
}
