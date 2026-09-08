// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// testEnv builds an Env whose manifests come from the checkout and whose
// cluster holds the given objects.
func testEnv(t *testing.T, cfg *config.Config, objects ...runtime.Object) *Env {
	t.Helper()
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	cfg.Root = root
	typed := fake.NewSimpleClientset(objects...)
	return &Env{Cfg: cfg, Kube: &kube.Client{Typed: typed}}
}

// otelConfigMap is the ate-otel-config ConfigMap of a cluster that was
// installed with the given mode and endpoint.
func otelConfigMap(mode, endpoint string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: otelConfigMapName, Namespace: NamespaceAteSystem},
		Data:       map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint},
	}
	if mode != "" {
		cm.Annotations = map[string]string{otelModeAnnotation: mode}
	}
	return cm
}

func namespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func service(namespace, name string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

// A mode that no flag gives comes from the cluster, thus a deploy of one
// component keeps the collector that the cluster has.
func TestResolveObservability(t *testing.T) {
	gkeEndpoint := endpointInFileForTest(t, config.ObservabilityGKE)
	kindEndpoint := endpointInFileForTest(t, config.ObservabilityKind)

	tests := []struct {
		name         string
		cfg          config.Config
		objects      []runtime.Object
		wantMode     string
		wantEndpoint string
	}{{
		name:     "no flag and no cluster gives none",
		wantMode: config.ObservabilityNone,
	}, {
		name:         "a kind install gives kind",
		cfg:          config.Config{Kind: true},
		wantMode:     config.ObservabilityKind,
		wantEndpoint: kindEndpoint,
	}, {
		name:         "the endpoint flag gives otlp",
		cfg:          config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		name:         "the mode flag wins over the cluster",
		cfg:          config.Config{Observability: config.ObservabilityNone},
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantMode:     config.ObservabilityNone,
		wantEndpoint: "",
	}, {
		name:         "no flag keeps the mode of the cluster",
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:         "no flag keeps the endpoint of an otlp cluster",
		objects:      []runtime.Object{otelConfigMap(config.ObservabilityOTLP, "http://collector.obs.svc:4317")},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		// An install that came before the modes has no annotation, thus the
		// endpoint is the only evidence of the collector it uses.
		name:         "a ConfigMap with no annotation keeps its collector",
		objects:      []runtime.Object{otelConfigMap("", gkeEndpoint)},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:         "a ConfigMap with no annotation and an unknown endpoint is otlp",
		objects:      []runtime.Object{otelConfigMap("", "http://collector.obs.svc:4317")},
		wantMode:     config.ObservabilityOTLP,
		wantEndpoint: "http://collector.obs.svc:4317",
	}, {
		name:     "a ConfigMap with no endpoint is none",
		objects:  []runtime.Object{otelConfigMap("", "")},
		wantMode: config.ObservabilityNone,
	}, {
		// A first install of a cluster that has the addon uses it. The mode
		// names the Service that this test finds, thus it cannot select a
		// collector that is absent.
		name: "the addon gives gke on a first install",
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode:     config.ObservabilityGKE,
		wantEndpoint: gkeEndpoint,
	}, {
		name:     "the namespace alone does not give gke",
		objects:  []runtime.Object{namespace(gkeOtelNamespace)},
		wantMode: config.ObservabilityNone,
	}, {
		name: "the mode of the cluster wins over the addon",
		objects: []runtime.Object{
			otelConfigMap(config.ObservabilityNone, ""),
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode: config.ObservabilityNone,
	}, {
		name: "a kind install does not use the addon",
		cfg:  config.Config{Kind: true},
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
		wantMode:     config.ObservabilityKind,
		wantEndpoint: kindEndpoint,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &tc.cfg, tc.objects...)
			got, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			if got.mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", got.mode, tc.wantMode)
			}
			if got.endpoint != tc.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", got.endpoint, tc.wantEndpoint)
			}
		})
	}
}

// Mode otlp takes the file of mode none, thus the endpoint and the two exporter
// switches in it must carry the given collector and not the empty default.
func TestRenderOtelConfigOTLP(t *testing.T) {
	e := testEnv(t, &config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"})
	got, err := e.ResolveObservability(context.Background())
	if err != nil {
		t.Fatalf("ResolveObservability: %v", err)
	}

	data, _, err := unstructured.NestedStringMap(got.obj.Object, "data")
	if err != nil {
		t.Fatalf("reading data: %v", err)
	}
	for key, want := range map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.obs.svc:4317",
		"OTEL_TRACES_EXPORTER":        "otlp",
		"OTEL_METRICS_EXPORTER":       "otlp",
	} {
		if data[key] != want {
			t.Errorf("data[%s] = %q, want %q", key, data[key], want)
		}
	}
	if got := got.obj.GetAnnotations()[otelModeAnnotation]; got != config.ObservabilityOTLP {
		t.Errorf("%s = %q, want %s", otelModeAnnotation, got, config.ObservabilityOTLP)
	}
}

// An absent collector must stop the install: it presents as a fault of the
// network, and not as an absent dependency.
func TestPreflightObservability(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Config
		objects []runtime.Object
		wantErr string
	}{{
		name:    "gke with no addon namespace fails",
		cfg:     config.Config{Observability: config.ObservabilityGKE},
		wantErr: "there is no namespace " + gkeOtelNamespace,
	}, {
		name:    "gke with the namespace but no Service fails",
		cfg:     config.Config{Observability: config.ObservabilityGKE},
		objects: []runtime.Object{namespace(gkeOtelNamespace)},
		wantErr: "there is no Service opentelemetry-collector",
	}, {
		name: "gke with the addon passes",
		cfg:  config.Config{Observability: config.ObservabilityGKE},
		objects: []runtime.Object{
			namespace(gkeOtelNamespace),
			service(gkeOtelNamespace, "opentelemetry-collector"),
		},
	}, {
		name:    "otlp with an absent in-cluster collector fails",
		cfg:     config.Config{OtlpEndpoint: "http://collector.obs.svc:4317"},
		wantErr: "there is no namespace obs",
	}, {
		// The installer cannot test an address outside the cluster.
		name: "otlp outside the cluster passes",
		cfg:  config.Config{OtlpEndpoint: "http://collector.example.com:4317"},
	}, {
		name: "none passes with no collector",
		cfg:  config.Config{Observability: config.ObservabilityNone},
	}, {
		// The collector of mode kind is in the bundle of the same install, thus
		// it does not exist before the install applies it.
		name: "kind passes before its collector exists",
		cfg:  config.Config{Kind: true},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &tc.cfg, tc.objects...)
			otel, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			err = e.preflightObservability(context.Background(), otel)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("preflightObservability: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("preflightObservability succeeded, want an error with %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("error = %v, want one with %q", err, tc.wantErr)
			}
		})
	}
}

// A restart is for a collector that changed only: it rolls every WorkerPool,
// which replaces the running actors.
func TestNoteOtelConfigChange(t *testing.T) {
	gkeEndpoint := endpointInFileForTest(t, config.ObservabilityGKE)

	tests := []struct {
		name        string
		objects     []runtime.Object
		wantChanged bool
	}{{
		name:        "a first install changes nothing",
		wantChanged: false,
	}, {
		name:        "the same collector changes nothing",
		objects:     []runtime.Object{otelConfigMap(config.ObservabilityNone, "")},
		wantChanged: false,
	}, {
		name:        "a different collector is a change",
		objects:     []runtime.Object{otelConfigMap(config.ObservabilityGKE, gkeEndpoint)},
		wantChanged: true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := testEnv(t, &config.Config{Observability: config.ObservabilityNone}, tc.objects...)
			otel, err := e.ResolveObservability(context.Background())
			if err != nil {
				t.Fatalf("ResolveObservability: %v", err)
			}
			e.noteOtelConfigChange(context.Background(), otel)
			if e.observability.changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", e.observability.changed, tc.wantChanged)
			}
			// A second deploy target in the same command must not find the
			// change again and restart the workloads a second time.
			e.observability.changed = false
			e.noteOtelConfigChange(context.Background(), otel)
			if e.observability.changed {
				t.Error("the second apply of the same ConfigMap reported a change")
			}
		})
	}
}

func TestEndpointService(t *testing.T) {
	tests := []struct {
		endpoint      string
		wantNamespace string
		wantService   string
	}{
		{"http://collector.obs.svc:4317", "obs", "collector"},
		{"http://collector.obs.svc.cluster.local:4317", "obs", "collector"},
		{"https://collector.obs.svc.cluster.local/v1/traces", "obs", "collector"},
		{"collector.obs.svc:4317", "obs", "collector"},
		{"http://collector.example.com:4317", "", ""},
		{"http://localhost:4317", "", ""},
		{"", "", ""},
	}
	for _, tc := range tests {
		gotNamespace, gotService := endpointService(tc.endpoint)
		if gotNamespace != tc.wantNamespace || gotService != tc.wantService {
			t.Errorf("endpointService(%q) = %q, %q, want %q, %q",
				tc.endpoint, gotNamespace, gotService, tc.wantNamespace, tc.wantService)
		}
	}
}

// endpointInFileForTest reads the endpoint of a mode from its manifest, thus
// the test asserts against the file and not against a copy of the address.
func endpointInFileForTest(t *testing.T, mode string) string {
	t.Helper()
	e := testEnv(t, &config.Config{})
	endpoint := endpointInFile(e.otelConfigPath(mode))
	if endpoint == "" {
		t.Fatalf("no endpoint in the manifest of mode %s", mode)
	}
	return endpoint
}
