#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The observability mode of hack/install-ate.sh, which sources this file.
# hack/verify/observability.sh sources it too, thus keep the functions here free
# of the state of the install.
#
# The telemetry stack is optional, because substrate must run on a cluster that
# has no collector. The mode is explicit for the same reason: a default that
# names one collector is correct on one type of cluster only, and it is wrong
# with no message on each other type. The modes are:
#
#   none   No collector. The components export no telemetry, and each one still
#          serves its own /metrics endpoint. This is the default.
#   otlp   The collector at --otlp-endpoint. Use this for a collector that you
#          operate, and for a measurement (read benchmarking/telemetry).
#   gke    The collector of the GKE managed OTel addon.
#   kind   The in-cluster collector that a kind install applies.
#
# Each mode supplies the ate-otel-config ConfigMap from a different file, and
# preflight_observability tests the mode before the install applies anything.

OTEL_CONFIG_NONE="manifests/ate-install/otel/none/ate-otel-config.yaml"
OTEL_CONFIG_GKE="manifests/ate-install/otel/gke/ate-otel-config.yaml"
OTEL_CONFIG_KIND="manifests/ate-install/otel/kind/ate-otel-config.yaml"

# The namespace and the Service of the GKE managed OTel addon. The addon makes
# both; the endpoint in ${OTEL_CONFIG_GKE} names both.
GKE_OTEL_NAMESPACE="gke-managed-otel"

# ate_observability echoes the selected mode, and stops the install if the mode
# is not a known one. With no --observability, --otlp-endpoint gives mode otlp,
# a kind install gives mode kind, and each other install gives mode none.
ate_observability() {
  local mode="${ATE_OBSERVABILITY:-}"
  if [[ -z "${mode}" ]]; then
    if [[ -n "${ATE_OTLP_ENDPOINT:-}" ]]; then
      mode="otlp"
    elif [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
      mode="kind"
    else
      mode="none"
    fi
  fi

  case "${mode}" in
    none | otlp | gke | kind)
      echo "${mode}"
      ;;
    *)
      echo "Error: --observability must be none, otlp, gke, or kind, got '${mode}'" >&2
      exit 1
      ;;
  esac
}

# validate_observability_flags rejects a combination of flags that no install
# can satisfy. Call it in the pre-scan, ahead of the first apply.
validate_observability_flags() {
  local mode
  mode="$(ate_observability)"

  if [[ "${mode}" == "otlp" && -z "${ATE_OTLP_ENDPOINT:-}" ]]; then
    echo "Error: --observability=otlp needs the address of a collector." >&2
    echo "  Give --otlp-endpoint URL, or select a different mode:" >&2
    echo "  --observability=gke for the GKE managed collector, or" >&2
    echo "  --observability=none to install with no telemetry export." >&2
    exit 1
  fi

  if [[ "${mode}" != "otlp" && -n "${ATE_OTLP_ENDPOINT:-}" ]]; then
    echo "Error: --otlp-endpoint is for --observability=otlp, but the mode is ${mode}." >&2
    echo "  Remove one of the two flags." >&2
    exit 1
  fi

  if [[ "${mode}" == "kind" && "${ATE_INSTALL_KIND:-false}" != "true" ]]; then
    echo "Error: --observability=kind needs a kind install, because the collector" >&2
    echo "  of that mode is the one that hack/install-ate-kind.sh applies." >&2
    echo "  Use hack/install-ate-kind.sh, or --observability=otlp with the" >&2
    echo "  address of your own collector." >&2
    exit 1
  fi

  validate_otlp_endpoint "${ATE_OTLP_ENDPOINT:-}"
}

# validate_otlp_endpoint tests the format of the URL of --otlp-endpoint. An
# empty value is correct, because each other mode reads its endpoint from a
# file. The set of permitted characters is small on purpose: the value goes into
# a ConfigMap and into the replacement of render_otel_config.
validate_otlp_endpoint() {
  local endpoint="$1"
  if [[ -z "${endpoint}" ]]; then
    return 0
  fi
  if [[ ! "${endpoint}" =~ ^https?://[A-Za-z0-9._-]+(:[0-9]+)?(/[A-Za-z0-9._~/-]*)?$ ]]; then
    echo "Error: --otlp-endpoint must be a URL with a scheme, for example" >&2
    echo "  http://opentelemetry-collector.otel-system.svc:4317" >&2
    echo "  Got '${endpoint}'." >&2
    exit 1
  fi
}

# otel_config_file echoes the manifest that supplies the ate-otel-config
# ConfigMap for the selected mode. Mode otlp uses the file of mode none, and
# render_otel_config puts the given endpoint in it.
otel_config_file() {
  case "$(ate_observability)" in
    kind) echo "${OTEL_CONFIG_KIND}" ;;
    gke) echo "${OTEL_CONFIG_GKE}" ;;
    *) echo "${OTEL_CONFIG_NONE}" ;;
  esac
}

# render_otel_config echoes the ate-otel-config ConfigMap of the selected mode.
# The endpoint is in the manifest before the apply, thus the install needs no
# patch after it, and no restart of the workloads that read the ConfigMap.
render_otel_config() {
  local file
  file="$(otel_config_file)"
  if [[ "$(ate_observability)" != "otlp" ]]; then
    cat "${file}"
    return
  fi
  sed "s|^  OTEL_EXPORTER_OTLP_ENDPOINT:.*|  OTEL_EXPORTER_OTLP_ENDPOINT: ${ATE_OTLP_ENDPOINT}|" \
    "${file}"
}

# rendered_otlp_endpoint echoes the endpoint of the ConfigMap above, or nothing
# in mode none. It reads the rendered manifest, thus the manifest stays the one
# source of the value.
rendered_otlp_endpoint() {
  render_otel_config \
    | sed -n 's|^  OTEL_EXPORTER_OTLP_ENDPOINT: *||p' \
    | tr -d '"'
}

# otlp_endpoint_service echoes "namespace service" for an endpoint that names a
# Service of this cluster, and nothing for each other address.
otlp_endpoint_service() {
  local host="$1"
  # Cut the scheme, then the path, then the port.
  host="${host#*://}"
  host="${host%%/*}"
  host="${host%%:*}"
  # A cluster address is service.namespace.svc, with or without cluster.local.
  if [[ "${host}" =~ ^([a-z0-9-]+)\.([a-z0-9-]+)\.svc(\.cluster\.local)?\.?$ ]]; then
    echo "${BASH_REMATCH[2]} ${BASH_REMATCH[1]}"
  fi
}

# check_otlp_endpoint_reachable tests that the Service of an in-cluster endpoint
# exists. An absent Service is an error: the components would fail to find the
# collector one time each minute, and the telemetry would stop with no message,
# which reads as a fault of the network and not as an absent dependency.
#
# The second argument is the line that tells the operator what to do. It differs
# with the mode, because the correction differs.
#
# An address outside the cluster gets a note only, because the installer cannot
# test it.
check_otlp_endpoint_reachable() {
  local endpoint="$1"
  local remedy="$2"
  local namespace="" service=""

  read -r namespace service <<<"$(otlp_endpoint_service "${endpoint}")"
  if [[ -z "${namespace}" ]]; then
    echo "  The installer does not test ${endpoint}, because the address is not one of this cluster."
    return 0
  fi

  if ! run_kubectl get namespace "${namespace}" >/dev/null 2>&1; then
    echo "Error: the collector at ${endpoint} is absent: there is no namespace ${namespace}." >&2
    echo "${remedy}" >&2
    exit 1
  fi
  if ! run_kubectl get service "${service}" --namespace "${namespace}" >/dev/null 2>&1; then
    echo "Error: the collector at ${endpoint} is absent: there is no Service ${service} in ${namespace}." >&2
    echo "${remedy}" >&2
    exit 1
  fi
}

# preflight_observability tests the selected mode, then reports it. It runs one
# time for each install: the deploy functions each apply the ConfigMap, and the
# operator needs the report one time only.
preflight_observability() {
  if [[ "${ATE_OBSERVABILITY_PREFLIGHT_DONE:-false}" == "true" ]]; then
    return 0
  fi
  ATE_OBSERVABILITY_PREFLIGHT_DONE=true

  local mode endpoint
  mode="$(ate_observability)"
  endpoint="$(rendered_otlp_endpoint)"

  case "${mode}" in
    none)
      echo "Observability: mode none. The control plane exports no telemetry."
      echo "  Each component still serves its own /metrics endpoint."
      if run_kubectl get namespace "${GKE_OTEL_NAMESPACE}" >/dev/null 2>&1; then
        echo "  The namespace ${GKE_OTEL_NAMESPACE} is present. To use that collector,"
        echo "  install again with --observability=gke."
      fi
      ;;
    gke)
      echo "Observability: mode gke, endpoint ${endpoint}"
      check_otlp_endpoint_reachable "${endpoint}" \
        "  Enable the managed OTel addon on the cluster, or install with
  --observability=otlp and the address of your own collector, or with
  --observability=none for no telemetry export."
      ;;
    otlp)
      echo "Observability: mode otlp, endpoint ${endpoint}"
      check_otlp_endpoint_reachable "${endpoint}" \
        "  Install the collector first, correct --otlp-endpoint, or install with
  --observability=none for no telemetry export."
      ;;
    kind)
      # No test of the Service: the collector is in the same bundle as the
      # components, thus it does not exist before this install applies it.
      echo "Observability: mode kind, endpoint ${endpoint}"
      ;;
  esac
}
