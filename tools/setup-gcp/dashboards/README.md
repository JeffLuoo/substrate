# Cloud Monitoring dashboards

Google Cloud Monitoring dashboard definitions for ATE. They turn the raw
`prometheus.googleapis.com/...` metrics that ATE emits into readable
per-method / per-stage latency / throughput / error views.

| File | Shows |
|------|-------|
| `ate-grpc-dashboard.json` | ateapi & atelet gRPC latency (p50/p95/p99), request rate, and error rate, by method |
| `ate-e2e-latency-dashboard.json` | "Substrate Routing & E2E Latency". Warm route P50/P95/P99 (`ate.router.resume="none"`), activation P50/P95/P99 (`ate.router.resume="triggered"`), activation P99 by stage (router / ateapi resume / atelet Restore), routing P99 by ActorTemplate and resume, routing QPS by outcome, plus the E2E full round-trip from Envoy (ms — includes actor compute + response, so it's context, not our overhead): P99 and QPS by response class. Needs the `atenet-router-envoy` PodMonitoring for the round-trip lines. |
| `ate-snapshot-dashboard.json` | Substrate snapshot image size and throughput ("Substrate Snapshot Size & QPS"): memory image size P99 by ActorTemplate, P50/P95/P99 for each memory image (gVisor `pages.img`, micro-VM `memory-ranges`), checkpoint QPS by ActorTemplate, and a heatmap of the gVisor `pages.img` size. Checkpoint/Restore *latency* is not here — it's the atelet gRPC `Checkpoint`/`Restore` methods in `ate-grpc-dashboard.json`. |

## Queries and the metric registry

Each query uses the metric, label, and label value names of
`docs/metrics/registry/metrics.yaml`. A quantile does not add together
the series that the registry tells you to divide: it constrains or
groups `atenet.router.route.duration` by `ate.router.resume`,
`ate.actor.lifecycle.operation.duration` by `ate.actor.operation.name`,
and `atelet.snapshot.size` by `file.name`. `TestDashboardsMatchTheRegistry`
in `tools/setup-gcp/cmd/dashboards_test.go` checks these rules.

## Applying

Dashboards are created/updated (idempotently) by setup:

```sh
go run ./tools/setup-gcp create dashboards   # also part of: bootstrap
```

Or apply any single file by hand:

```sh
gcloud monitoring dashboards create --config-from-file=tools/setup-gcp/dashboards/<file>.json
```
