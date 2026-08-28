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

# Tests the --observability mode of hack/install-ate.sh: the selection of the
# mode, the tests of the flags, the ate-otel-config ConfigMap of each mode, and
# the preflight. The test needs no cluster: it gives hack/observability.sh a
# run_kubectl that answers from a list.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# The objects that the run_kubectl below reports as present. Each test sets it.
FAKE_OBJECTS=""

# run_kubectl answers `get <kind> <name>` and `get <kind> <name> --namespace ns`
# from FAKE_OBJECTS, which holds one "kind/name" or "kind/name.namespace" for
# each object. Each other command is an error, because no function under test
# is permitted to change the cluster.
run_kubectl() {
  local kind="" name="" namespace="" key=""
  if [[ "${1:-}" != "get" ]]; then
    echo "run_kubectl: the test permits get only, got: $*" >&2
    return 1
  fi
  kind="$2"
  name="$3"
  shift 3
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --namespace)
        namespace="$2"
        shift
        ;;
      --namespace=*) namespace="${1#*=}" ;;
    esac
    shift
  done

  key="${kind}/${name}"
  if [[ -n "${namespace}" ]]; then
    key="${key}.${namespace}"
  fi
  [[ " ${FAKE_OBJECTS} " == *" ${key} "* ]]
}

source "${ROOT}"/hack/observability.sh

failures=0

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [[ "${want}" == "${got}" ]]; then
    echo "ok   ${name}"
    return 0
  fi
  echo "FAIL ${name}: want '${want}', got '${got}'"
  failures=$((failures + 1))
}

# expect_ok and expect_error run the given command in a subshell, because a
# function under test reports a fault of the operator with exit.
expect_ok() {
  local name="$1"
  shift
  if ("$@" >/dev/null 2>&1); then
    echo "ok   ${name}"
    return 0
  fi
  echo "FAIL ${name}: want success, got an error"
  failures=$((failures + 1))
}

expect_error() {
  local name="$1"
  shift
  if ("$@" >/dev/null 2>&1); then
    echo "FAIL ${name}: want an error, got success"
    failures=$((failures + 1))
    return 0
  fi
  echo "ok   ${name}"
}

# The mode, with and without the flag.
ATE_OBSERVABILITY="" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_eq "default mode is none" "none" "$(ate_observability)"

ATE_INSTALL_KIND=true
expect_eq "a kind install defaults to mode kind" "kind" "$(ate_observability)"

ATE_INSTALL_KIND=false ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317"
expect_eq "--otlp-endpoint gives mode otlp" "otlp" "$(ate_observability)"

ATE_OTLP_ENDPOINT="" ATE_OBSERVABILITY="gke"
expect_eq "the flag selects the mode" "gke" "$(ate_observability)"

ATE_OBSERVABILITY="prometheus"
expect_error "an unknown mode is an error" ate_observability

# The tests of the flags.
ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT=""
expect_error "mode otlp with no endpoint is an error" validate_observability_flags

ATE_OBSERVABILITY="gke" ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317"
expect_error "an endpoint with a different mode is an error" validate_observability_flags

ATE_OBSERVABILITY="kind" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_error "mode kind outside a kind install is an error" validate_observability_flags

ATE_OBSERVABILITY="kind" ATE_INSTALL_KIND=true
expect_ok "mode kind in a kind install is correct" validate_observability_flags

ATE_INSTALL_KIND=false ATE_OBSERVABILITY="none"
expect_ok "mode none needs no other flag" validate_observability_flags

ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="collector.otel-system.svc:4317"
expect_error "an endpoint with no scheme is an error" validate_observability_flags

ATE_OTLP_ENDPOINT="http://collector.otel-system.svc:4317/v1"
expect_ok "an endpoint with a path is correct" validate_observability_flags

ATE_OTLP_ENDPOINT='http://collector.svc:4317;rm -rf /'
expect_error "an endpoint with a shell character is an error" validate_observability_flags

# The ConfigMap of each mode.
ATE_OBSERVABILITY="none" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
expect_eq "mode none names no collector" "" "$(rendered_otlp_endpoint)"
expect_eq "mode none uses the file of mode none" \
  "${OTEL_CONFIG_NONE}" "$(otel_config_file)"

ATE_OBSERVABILITY="gke"
expect_eq "mode gke names the addon collector" \
  "http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317" \
  "$(rendered_otlp_endpoint)"

ATE_OBSERVABILITY="kind" ATE_INSTALL_KIND=true
expect_eq "mode kind names the in-cluster collector" \
  "http://opentelemetry-collector.otel-system.svc:4317" "$(rendered_otlp_endpoint)"
expect_eq "mode kind keeps the export interval of the kind file" \
  "10000" "$(render_otel_config | sed -n 's|^  OTEL_METRIC_EXPORT_INTERVAL: *||p' | tr -d '"')"

ATE_OBSERVABILITY="otlp" ATE_INSTALL_KIND=false
ATE_OTLP_ENDPOINT="http://telemetry-meter.benchmarking.svc.cluster.local:4317"
expect_eq "mode otlp names the given collector" \
  "${ATE_OTLP_ENDPOINT}" "$(rendered_otlp_endpoint)"
expect_eq "the rendered ConfigMap keeps its name" \
  "  name: ate-otel-config" "$(render_otel_config | grep '^  name:')"

# The Service of an endpoint.
expect_eq "an endpoint of this cluster gives its Service" \
  "otel-system opentelemetry-collector" \
  "$(otlp_endpoint_service "http://opentelemetry-collector.otel-system.svc:4317")"
expect_eq "cluster.local gives the same Service" \
  "otel-system opentelemetry-collector" \
  "$(otlp_endpoint_service "http://opentelemetry-collector.otel-system.svc.cluster.local:4317")"
expect_eq "an address outside the cluster gives nothing" \
  "" "$(otlp_endpoint_service "https://otlp.example.com:443")"

# The preflight.
ATE_OBSERVABILITY="gke" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=false
ATE_OBSERVABILITY_PREFLIGHT_DONE=false FAKE_OBJECTS=""
expect_error "mode gke without the addon stops the install" preflight_observability

FAKE_OBJECTS="namespace/gke-managed-otel"
expect_error "mode gke without the Service of the addon stops the install" preflight_observability

FAKE_OBJECTS="namespace/gke-managed-otel service/opentelemetry-collector.gke-managed-otel"
expect_ok "mode gke with the addon is correct" preflight_observability

ATE_OBSERVABILITY="otlp" ATE_OTLP_ENDPOINT="http://my-collector.my-ns.svc:4317"
FAKE_OBJECTS="namespace/my-ns"
expect_error "mode otlp without the collector stops the install" preflight_observability

FAKE_OBJECTS="namespace/my-ns service/my-collector.my-ns"
expect_ok "mode otlp with the collector is correct" preflight_observability

ATE_OTLP_ENDPOINT="https://otlp.example.com:443" FAKE_OBJECTS=""
expect_ok "mode otlp with an address outside the cluster is correct" preflight_observability

ATE_OBSERVABILITY="kind" ATE_OTLP_ENDPOINT="" ATE_INSTALL_KIND=true FAKE_OBJECTS=""
expect_ok "mode kind does not test the collector that it applies" preflight_observability

ATE_OBSERVABILITY="none" ATE_INSTALL_KIND=false
expect_ok "mode none tests nothing" preflight_observability

# The preflight reports one time for each install, because each deploy function
# applies the ConfigMap.
ATE_OBSERVABILITY_PREFLIGHT_DONE=false
preflight_observability >/dev/null
expect_eq "the preflight reports one time" "" "$(preflight_observability)"

if ((failures > 0)); then
  echo "${failures} test(s) failed" >&2
  exit 1
fi
echo "All observability tests passed"
