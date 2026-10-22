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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kustomize"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

const (
	// installDir is the manifest root, relative to the repository root.
	installDir = "manifests/ate-install"
	// Envoy dataplane image name
	envoyDataplaneImage = "envoy-dataplane"
	// Envoy dataplane Dockerfile path
	envoyDataplaneDockefile = "cmd/dataplane/envoy"
)

// cordonControlPlaneComponent is the kustomize component that pins each
// control plane workload to its own node, layered over every control plane
// apply under --cordon-control-plane.
const cordonControlPlaneComponent = installDir + "/components/cordon-control-plane"

// fakeDataPlaneComponent is the kustomize component that puts fake-atelet in
// atelet's DaemonSet and fake-workersync in place of ate-controller, layered
// over the atelet and ate-controller applies under --fake-data-plane.
const fakeDataPlaneComponent = installDir + "/components/fake-data-plane"

// routerStaticMTLSComponent is the kustomize component that switches the
// ingress router to --ingress-auth-mode=static-mtls, layered over every
// control plane apply under --ingress-auth-mode=static-mtls.
const routerStaticMTLSComponent = installDir + "/components/router-static-mtls"

// components lists the kustomize components every control plane apply is
// composed with. Each patch targets its workloads by name, and kustomize leaves
// a stream alone when nothing in it matches, so composing a manifest that
// carries none of them is harmless.
func (e *Env) components() []string {
	var components []string
	if e.Cfg.FakeDataPlane {
		components = append(components, e.Cfg.Path(fakeDataPlaneComponent))
	}
	if e.Cfg.IngressAuthMode == config.IngressAuthStaticMTLS {
		components = append(components, e.Cfg.Path(routerStaticMTLSComponent))
	}
	if e.Cfg.CordonControlPlane {
		components = append(components, e.Cfg.Path(cordonControlPlaneComponent))
	}
	return components
}

// SystemOverlay picks the kustomization for a full control plane install.
//
// The choice is a product of two switches: kind vs GKE, and the atenet router
// dataplane. The plain GKE envoy install renders the base kustomization rather
// than the raw manifests/ate-install directory: the directory would also
// re-apply pod-certificate-controller.yaml (reverting the size10 flags and the
// WORKERS_PER_SIGNER value set earlier in the install), both atenet-egress
// variants, and the sandboxconfig files, all of which have their own apply
// steps.
func SystemOverlay(cfg *config.Config) string {
	switch {
	case cfg.Router == config.RouterAgentgateway && cfg.Kind:
		return installDir + "/kind-agentgateway"
	case cfg.Router == config.RouterAgentgateway:
		return installDir + "/agentgateway"
	case cfg.Kind:
		return installDir + "/kind"
	default:
		return installDir + "/base"
	}
}

// kustomization directory, before image resolution. It composes path with the
// selected components (see components), so the same patches reach every
// control plane workload whichever apply path delivers it.
func (e *Env) render(path string) ([]byte, error) {
	if components := e.components(); len(components) > 0 {
		return kustomize.Compose(path, components...)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	if info.IsDir() {
		return kustomize.Build(path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	return data, nil
}

// renderBytes is render for a manifest already held in memory.
func (e *Env) renderBytes(manifest []byte) ([]byte, error) {
	if components := e.components(); len(components) > 0 {
		return kustomize.ComposeBytes(manifest, components...)
	}
	return manifest, nil
}

// renderResolve renders path and resolves its image references.
func (e *Env) renderResolve(ctx context.Context, path string) ([]byte, error) {
	manifest, err := e.render(path)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, manifest)
}

// renderResolveApply renders path, resolves its images, and applies the result.
func (e *Env) renderResolveApply(ctx context.Context, path string) error {
	manifest, err := e.renderResolve(ctx, path)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, manifest)
}

// renderSystemManifests produces the full control plane manifest with all
// image references resolved.
func (e *Env) renderSystemManifests(ctx context.Context) ([]byte, error) {
	manifest, err := e.render(e.Cfg.Path(SystemOverlay(e.Cfg)))
	if err != nil {
		return nil, err
	}
	if e.Cfg.Router != config.RouterAgentgateway {
		imageReference, err := e.dockerfileImage(ctx, envoyDataplaneImage, envoyDataplaneDockefile)
		if err != nil {
			return nil, err
		}
		manifest = e.patchEnvoyDataplaneImage(manifest, imageReference)
	}
	return e.ResolveManifestBytes(ctx, manifest)
}

// renderAtenetRouterManifest produces the atenet router manifest for the
// selected dataplane.
func (e *Env) renderAtenetRouterManifest(ctx context.Context) ([]byte, error) {
	if e.Cfg.Router == config.RouterAgentgateway {
		return e.renderResolve(ctx, e.Cfg.Path(installDir+"/agentgateway-router"))
	}
	manifest, err := e.render(e.Cfg.Manifest("atenet-router.yaml"))
	if err != nil {
		return nil, err
	}
	imageReference, err := e.dockerfileImage(ctx, envoyDataplaneImage, envoyDataplaneDockefile)
	if err != nil {
		return nil, err
	}
	manifest = e.patchEnvoyDataplaneImage(manifest, imageReference)
	return e.ResolveManifestBytes(ctx, manifest)
}

// atenetEgressManifestPath returns the envoy egress gateway manifest.
func (e *Env) atenetEgressManifestPath() string {
	return e.Cfg.Manifest("atenet-egress.yaml")
}

// renderAtenetEgressManifest produces the atenet egress manifest.
func (e *Env) renderAtenetEgressManifest(ctx context.Context, provider config.CredentialProvider) ([]byte, error) {
	general := e.Cfg.AdditionalEgressExtprocService != ""

	if e.Cfg.Router == config.RouterAgentgateway {
		raw, err := e.render(e.Cfg.Path(installDir + "/agentgateway-egress"))
		if err != nil {
			return nil, err
		}
		raw, err = patchAgentgatewayEgressInject(raw, provider)
		if err != nil {
			return nil, err
		}
		raw, err = patchAgentgatewayEgressExtproc(raw, e.Cfg.AdditionalEgressExtprocService)
		if err != nil {
			return nil, err
		}
		return e.ResolveManifestBytes(ctx, raw)
	}

	imageReference, err := e.dockerfileImage(ctx, envoyDataplaneImage, envoyDataplaneDockefile)
	if err != nil {
		return nil, err
	}

	// The general additional-ext_proc filter and egress credential injection are
	// independent splices with their own markers, so compose them. The cordon
	// render comes last, over the spliced stream, so the node pinning reaches
	// every variant.
	var raw []byte
	if general {
		raw, err = e.patchAtenetEgressManifest()
	} else {
		raw, err = os.ReadFile(e.atenetEgressManifestPath())
	}
	if err != nil {
		return nil, err
	}
	raw, err = e.patchAtenetEgressInject(raw, provider)
	if err != nil {
		return nil, err
	}
	raw = e.patchEnvoyDataplaneImage(raw, imageReference)
	if e.Cfg.EnvoyConcurrency > 0 {
		raw, err = patchAtenetEgressEnvoyConcurrency(raw, e.Cfg.EnvoyConcurrency)
		if err != nil {
			return nil, err
		}
	}
	rendered, err := e.renderBytes(raw)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, rendered)
}

// patchAtenetEgressEnvoyConcurrency uses a kustomize JSON6902 patch to append
// --concurrency <concurrency> to the envoy container's args in the
// atenet-egress Deployment.
func patchAtenetEgressEnvoyConcurrency(raw []byte, concurrency int) ([]byte, error) {
	dir, err := os.MkdirTemp("", "ate-setup-kustomize-")
	if err != nil {
		return nil, fmt.Errorf("while creating a scratch kustomization: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), raw, 0o600); err != nil {
		return nil, fmt.Errorf("while staging a manifest for kustomize: %w", err)
	}
	kustomization := fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - manifest.yaml
patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: atenet-egress
      namespace: ate-system
    patch: |-
      - op: test
        path: /spec/template/spec/containers/0/name
        value: envoy
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --concurrency
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: %q
`, strconv.Itoa(concurrency))
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o600); err != nil {
		return nil, fmt.Errorf("while writing a scratch kustomization: %w", err)
	}
	return kustomize.Build(dir)
}

// patchEnvoyDataplaneImage replaces the ${ENVOY_DATAPLANE_IMAGE} placeholder in
// the manifest with imageRef.
func (e *Env) patchEnvoyDataplaneImage(raw []byte, imageRef string) []byte {
	return bytes.ReplaceAll(raw, []byte("${ENVOY_DATAPLANE_IMAGE}"), []byte(imageRef))
}

// patchAtenetEgressInject replaces the #ATE_EGRESS_INJECT_FLAGS marker in the
// egress sidecar's args with the credential-provider flags, or removes it when
// injection is off, which leaves the gateway with no provider. It takes the
// manifest bytes so it can run after the general patch.
func (e *Env) patchAtenetEgressInject(raw []byte, provider config.CredentialProvider) ([]byte, error) {
	var flagsBlock string
	if provider.Enabled() {
		flagsBlock = emitEgressInjectFlags(provider.Name, provider.Address, provider.ServerName())
	}
	return replaceManifestMarker(raw, "#ATE_EGRESS_INJECT_FLAGS", flagsBlock)
}

// The HTTP and HTTPS listeners share the egress policy and its providers.
func patchAgentgatewayEgressInject(raw []byte, provider config.CredentialProvider) ([]byte, error) {
	var block string
	if provider.Enabled() {
		block = fmt.Sprintf(`credentialProviders:
- uriAuthority: %q
  target:
    host: %q
    policies:
      backendTLS:
        hostname: %q
        cert: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        key: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        root: /run/servicedns-ca/trust-bundle.pem`, provider.Name, provider.Address, provider.ServerName())
	}
	return replaceManifestMarker(raw, "#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS", block)
}

// patchAgentgatewayEgressExtproc splices an extProc policy onto the HTTP and
// HTTPS egress routes when an additional ext_proc service is configured,
// forwarding both source.spiffeId and source.certificate so the processor can
// verify the actor's UID from the ActorIdentity X.509 extension.
func patchAgentgatewayEgressExtproc(raw []byte, serviceSpec string) ([]byte, error) {
	var block string
	if serviceSpec != "" {
		parts := strings.Split(serviceSpec, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid additional ext_proc service %q: expected <namespace>/<service>:<port>", serviceSpec)
		}
		namespace := parts[0]
		svcPort := strings.Split(parts[1], ":")
		if len(svcPort) != 2 {
			return nil, fmt.Errorf("invalid additional ext_proc service %q: expected <namespace>/<service>:<port>", serviceSpec)
		}
		service := svcPort[0]
		port := svcPort[1]
		address := fmt.Sprintf("%s.%s.svc.cluster.local:%s", service, namespace, port)
		serverName := fmt.Sprintf("%s.%s.svc", service, namespace)
		block = fmt.Sprintf(`extProc:
  host: %q
  policies:
    backendTLS:
      hostname: %q
      cert: /run/podidentity.podcert.ate.dev/credential-bundle.pem
      key: /run/podidentity.podcert.ate.dev/credential-bundle.pem
      root: /run/servicedns-ca/trust-bundle.pem
  failureMode: failClosed
  processingOptions:
    requestHeaderMode: send
    responseHeaderMode: skip
    requestBodyMode: none
    responseBodyMode: none
    requestTrailerMode: skip
    responseTrailerMode: skip
  requestAttributes:
    source.spiffeId: source.spiffeId
    source.certificate: source.certificate`, address, serverName)
	}
	return replaceManifestMarkers(raw, "#ATE_AGENTGATEWAY_EXTPROC", block, 2)
}

func replaceManifestMarker(raw []byte, marker, block string) ([]byte, error) {
	return replaceManifestMarkers(raw, marker, block, 1)
}

func replaceManifestMarkers(raw []byte, marker, block string, want int) ([]byte, error) {
	var out []string
	replaced := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), marker) {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			if block != "" {
				for _, l := range strings.Split(block, "\n") {
					out = append(out, indent+l)
				}
			}
			replaced++
			continue
		}
		out = append(out, line)
	}
	if replaced != want {
		return nil, fmt.Errorf("expected %d %s marker, found %d", want, marker, replaced)
	}
	return []byte(strings.Join(out, "\n")), nil
}

func emitEgressInjectFlags(name, address, serverName string) string {
	return fmt.Sprintf(`- --credential-provider-name=%s
- --credential-provider-address=%s
- --credential-provider-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.ate.dev/credential-bundle.pem
- --credential-provider-server-name=%s`, name, address, serverName)
}

func (e *Env) patchAtenetEgressManifest() ([]byte, error) {
	raw, err := os.ReadFile(e.atenetEgressManifestPath())
	if err != nil {
		return nil, fmt.Errorf("reading egress manifest: %w", err)
	}

	spec := e.Cfg.AdditionalEgressExtprocService
	parts := strings.Split(spec, "/")
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	service := svcPort[0]
	port := svcPort[1]

	address := fmt.Sprintf("%s.%s.svc.cluster.local", service, namespace)
	serverName := fmt.Sprintf("%s.%s.svc", service, namespace)

	filterBlock := emitAdditionalEgressExtprocFilter()
	if path := e.Cfg.AdditionalEgressExtprocFiltersFile; path != "" {
		filterBlock, err = loadAdditionalEgressExtprocFilters(path)
		if err != nil {
			return nil, err
		}
	}
	clusterBlock := emitAdditionalEgressExtprocCluster(address, port, serverName)

	lines := strings.Split(string(raw), "\n")
	var out []string
	filtersReplaced := 0
	clustersReplaced := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_FILTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, fLine := range strings.Split(filterBlock, "\n") {
				if fLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+fLine)
				}
			}
			filtersReplaced++
			continue
		}
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_CLUSTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, cLine := range strings.Split(clusterBlock, "\n") {
				if cLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+cLine)
				}
			}
			clustersReplaced++
			continue
		}
		out = append(out, line)
	}

	if filtersReplaced != 2 || clustersReplaced != 1 {
		return nil, fmt.Errorf("expected 2 filter markers and 1 cluster marker in %s, found %d and %d",
			e.atenetEgressManifestPath(), filtersReplaced, clustersReplaced)
	}

	return []byte(strings.Join(out, "\n")), nil
}

const additionalEgressExtprocCluster = "additional_egress_ext_proc"

// actorIdentityAttribute is the request attribute that names the actor. Every
// additional ext_proc filter must request it, so that the processor never
// has to identify the actor from headers that the sandbox can set.
const actorIdentityAttribute = "filter_state['dev.ate.actor.identity']"

// extProcFilterType is the typed_config of an ext_proc HTTP filter.
const extProcFilterType = "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor"

const (
	extProcFilterName = "envoy.filters.http.ext_proc"
	// matchDelegateFilterName wraps one HTTP filter with a matcher that can
	// skip it per request.
	matchDelegateFilterName = "envoy.filters.http.match_delegate"
	// extensionWithMatcherType is the typed_config of a match_delegate.
	extensionWithMatcherType = "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcher"
)

// loadAdditionalEgressExtprocFilters reads the filters an operator splices in
// place of emitAdditionalEgressExtprocFilter: for example a synchronous filter
// that may hold a request on its headers, followed by an observability-mode
// filter that streams response bodies. It returns them as a YAML list for the
// filter markers.
//
// Each filter must be an ext_proc filter that dials the
// additional_egress_ext_proc cluster and requests the actor identity
// attribute, or a match_delegate that wraps exactly one such filter. A filter
// that is not in observability mode must also set
// mutation_rules.disallow_system: true. A match_delegate lets its matcher
// skip the processor for requests it does not need (for example every host
// other than the LLM providers). The file configures what the additional
// processor sees and how long Envoy waits for it; it cannot add other filters
// to the egress chains or point them at another cluster.
func loadAdditionalEgressExtprocFilters(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading additional ext_proc filters: %w", err)
	}
	var filters []map[string]any
	if err := yaml.UnmarshalStrict(raw, &filters); err != nil {
		return "", fmt.Errorf("additional ext_proc filters in %s: want a YAML list of filters: %w", path, err)
	}
	if len(filters) == 0 {
		return "", fmt.Errorf("additional ext_proc filters in %s: the list is empty", path)
	}
	for i, f := range filters {
		if name, _ := f["name"].(string); name == matchDelegateFilterName {
			inner, err := matchDelegateExtension(f)
			if err != nil {
				return "", fmt.Errorf("additional ext_proc filters in %s: filter %d: %w", path, i+1, err)
			}
			f = inner
		}
		if name, _ := f["name"].(string); name != extProcFilterName {
			return "", fmt.Errorf("additional ext_proc filters in %s: filter %d is %q, want %s or a %s that wraps one", path, i+1, name, extProcFilterName, matchDelegateFilterName)
		}
		typedConfig, _ := f["typed_config"].(map[string]any)
		if got, _ := typedConfig["@type"].(string); got != extProcFilterType {
			return "", fmt.Errorf("additional ext_proc filters in %s: filter %d has typed_config @type %q, want %s", path, i+1, got, extProcFilterType)
		}
		if got := extProcCluster(f); got != additionalEgressExtprocCluster {
			return "", fmt.Errorf("additional ext_proc filters in %s: filter %d dials %q, want grpc_service.envoy_grpc.cluster_name %s",
				path, i+1, got, additionalEgressExtprocCluster)
		}
		if !slices.Contains(stringList(typedConfig["request_attributes"]), actorIdentityAttribute) {
			return "", fmt.Errorf("additional ext_proc filters in %s: filter %d does not request the attribute %s",
				path, i+1, actorIdentityAttribute)
		}
		// Envoy applies no mutation from a filter in observability mode. Any
		// other filter must not change system headers such as :authority,
		// which select the upstream and the credential that is injected.
		mutationRules, _ := typedConfig["mutation_rules"].(map[string]any)
		if typedConfig["observability_mode"] != true && mutationRules["disallow_system"] != true {
			return "", fmt.Errorf("additional ext_proc filters in %s: filter %d can mutate requests but does not set mutation_rules.disallow_system: true",
				path, i+1)
		}
	}
	out, err := yaml.Marshal(filters)
	if err != nil {
		return "", fmt.Errorf("additional ext_proc filters in %s: %w", path, err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}

// stringList returns the strings of a YAML list, or nil.
func stringList(v any) []string {
	items, _ := v.([]any)
	var out []string
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// matchDelegateExtension returns the one filter a match_delegate wraps. Its
// typed_config may hold only the wrapped filter (extension_config) and the
// matcher (xds_matcher); Envoy lets that matcher do nothing but skip the
// wrapped filter.
func matchDelegateExtension(filter map[string]any) (map[string]any, error) {
	typedConfig, _ := filter["typed_config"].(map[string]any)
	if got, _ := typedConfig["@type"].(string); got != extensionWithMatcherType {
		return nil, fmt.Errorf("%s has typed_config @type %q, want %s", matchDelegateFilterName, got, extensionWithMatcherType)
	}
	for key := range typedConfig {
		switch key {
		case "@type", "extension_config", "xds_matcher":
		default:
			return nil, fmt.Errorf("%s has typed_config field %q; only extension_config and xds_matcher are allowed", matchDelegateFilterName, key)
		}
	}
	if _, ok := typedConfig["xds_matcher"].(map[string]any); !ok {
		return nil, fmt.Errorf("%s has no xds_matcher", matchDelegateFilterName)
	}
	inner, ok := typedConfig["extension_config"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s has no extension_config", matchDelegateFilterName)
	}
	return inner, nil
}

// extProcCluster returns the cluster an ext_proc filter dials, directly or
// inside a match_delegate, or "" for any other filter.
func extProcCluster(filter map[string]any) string {
	if filter["name"] == matchDelegateFilterName {
		inner, err := matchDelegateExtension(filter)
		if err != nil {
			return ""
		}
		filter = inner
	}
	if filter["name"] != extProcFilterName {
		return ""
	}
	typedConfig, _ := filter["typed_config"].(map[string]any)
	grpcService, _ := typedConfig["grpc_service"].(map[string]any)
	envoyGRPC, _ := grpcService["envoy_grpc"].(map[string]any)
	name, _ := envoyGRPC["cluster_name"].(string)
	return name
}

func emitAdditionalEgressExtprocFilter() string {
	return `- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: additional_egress_ext_proc
      timeout: 2s
    failure_mode_allow: false
    message_timeout: 2s
    request_attributes:
    - filter_state['dev.ate.actor.identity']
    - filter_state['dev.ate.actor.uid']
    - filter_state['dev.ate.actor.template']
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
      request_trailer_mode: SKIP
      response_trailer_mode: SKIP
    mutation_rules:
      disallow_system: true
      disallow_is_error: true`
}

func emitAdditionalEgressExtprocCluster(address, port, serverName string) string {
	return fmt.Sprintf(`- name: %s
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  connect_timeout: 1s
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {}
  transport_socket:
    name: envoy.transport_sockets.tls
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
      sni: %s
      common_tls_context:
        tls_params:
          tls_minimum_protocol_version: TLSv1_3
          tls_maximum_protocol_version: TLSv1_3
        tls_certificate_sds_secret_configs:
        - name: podidentity_client_cert
          sds_config:
            resource_api_version: V3
            path_config_source:
              path: /etc/envoy/sds-podidentity-cert.yaml
        combined_validation_context:
          default_validation_context:
            match_typed_subject_alt_names:
            - san_type: DNS
              matcher:
                exact: %s
          validation_context_sds_secret_config:
            name: servicedns_validation_context
            sds_config:
              resource_api_version: V3
              path_config_source:
                path: /etc/envoy/sds-servicedns-validation.yaml
  load_assignment:
    cluster_name: %s
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: %s
              port_value: %s`, additionalEgressExtprocCluster, serverName, serverName, additionalEgressExtprocCluster, address, port)
}

func (e *Env) applyAtenetEgress(ctx context.Context, provider config.CredentialProvider) error {
	manifests, err := e.renderAtenetEgressManifest(ctx, provider)
	if err != nil {
		return err
	}

	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), "atenet-egress")
	if err != nil {
		return err
	}

	if err := e.Kube.ApplyBytes(ctx, manifests); err != nil {
		return err
	}

	if running && (e.Cfg.AdditionalEgressExtprocService != "" || provider.Enabled()) {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), "atenet-egress", time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// otelConfigPath returns the environment's ate-otel-config ConfigMap.
//
// Every control plane component pulls this ConfigMap in via envFrom. A full
// install gets it as part of the rendered bundle, but the single-component
// redeploys apply raw manifests with no kustomize, so they have to select the
// right copy themselves: applying the base file on a kind cluster would
// overwrite it with the GKE endpoint and break telemetry everywhere at once.
func (e *Env) otelConfigPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "ate-otel-config.yaml")
	}
	return e.Cfg.Manifest("ate-otel-config.yaml")
}

// applyOtelConfig applies the environment's ate-otel-config ConfigMap.
func (e *Env) applyOtelConfig(ctx context.Context) error {
	return e.Kube.ApplyPath(ctx, e.otelConfigPath())
}

// otelConfigMap is the ConfigMap every control plane component reads its
// telemetry settings from through envFrom.
const otelConfigMap = "ate-otel-config"

// otelEndpointKey is the collector address inside it.
const otelEndpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT"

// otelOverrideDeployments are the control plane Deployments that read
// ate-otel-config. ate-controller additionally copies the values onto the
// worker pods it creates, so one patch reaches the whole system.
var otelOverrideDeployments = []string{"ate-api-server", "ate-controller", "atenet-router"}

// applyOtelEndpointOverride points all control plane telemetry at a different
// collector for the duration of a measurement. See
// benchmarking/telemetry/README.md.
//
// Call this AFTER every apply: the ate-system bundle carries its own copy of
// ate-otel-config, so applying it replaces an earlier patch and the endpoint
// silently returns to the cluster default.
//
// A ConfigMap change starts no rollout, because the pod template stays the
// same, so the consumers have to be restarted. Only on an actual change: a
// restart during the bundle's rollout makes the two compete, and the rollout
// wait can then exceed its timeout. An absent workload is not an error,
// because a single-component deploy has only that component.
func (e *Env) applyOtelEndpointOverride(ctx context.Context) error {
	endpoint := e.Cfg.OtlpEndpoint
	if endpoint == "" {
		return nil
	}

	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), otelConfigMap)
	if err != nil {
		return err
	}
	if cm != nil && cm.Data[otelEndpointKey] == endpoint {
		return nil
	}

	log.Infof("Overriding %s with %s", otelEndpointKey, endpoint)
	if err := e.Kube.MergePatchConfigMap(ctx, e.Namespace(), otelConfigMap,
		map[string]string{otelEndpointKey: endpoint}); err != nil {
		return err
	}

	now := time.Now()
	for _, name := range otelOverrideDeployments {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), name, now); err != nil {
			return err
		}
	}
	return e.Kube.RolloutRestart(ctx, e.Namespace(), "atelet", now)
}
