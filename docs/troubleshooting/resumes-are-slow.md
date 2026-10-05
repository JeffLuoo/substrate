# Resumes are slow

> Some documents call this a cold start. Substrate says **resume**, because the
> usual path restores a snapshot. It does not boot the actor from nothing.

## Context

A **resume** is the activation of an actor that is not on a worker. An actor is
idle most of the time, and Substrate suspends it to release its worker. The
next request must put the actor back on a worker before the actor can answer.
The request waits for that. This is the `triggered` series of
[requests-are-slow.md](requests-are-slow.md), measured from the other end.

Two states give a resume, and their cost is not the same:

| State before | Where the snapshot is | Cost |
|---|---|---|
| Paused | On the node VM. The resume accepts only a worker on that VM. | Low. No download. |
| Suspended | In object storage (GCS or S3) | High. A download and an unpack. |

`ate.snapshot.scope` also changes the cost. A `data` snapshot holds only the
durable directories, thus the resume starts the containers again from the
image and the actor pays its startup. A `full` snapshot also holds the memory
of the guest.

A pause also pins the actor: the resume that follows accepts only a worker on
the node that holds the snapshot. Refer to
[capacity-is-full.md](capacity-is-full.md) when a pinned actor waits while the
pool has free workers.

`ate.snapshot.kind` tells you which snapshot the resume read. The permitted
values and their meaning are in the `registry.ate.snapshot` group of
[the registry](../metrics/registry/metrics.yaml). One of them changes what you
can query: a `boot` is a start from nothing, thus it is not a restore and the
atelet restore histogram has no data for it.

Two instruments measure a resume. They do not measure the same part:

| Metric | Emitted by | What it covers |
|---|---|---|
| `ate.actor.lifecycle.operation.duration` with `ate.actor.operation.name="resume"` | ateapi | The full operation, with the scheduler. |
| `ate.actor.restore.duration` | atelet | Only the part on the worker node. |

If the ateapi number is much larger than the atelet `total` phase, the delay is
before the handoff. Examine the scheduler in step 5, and read the blind spots.

**Do not add the phases together.** The download occurs at the same time as the
asset fetch and the OCI unpack. Each phase is an independent measurement. Use
`total` as the denominator. A phase that did not start is absent. It is not
zero.

Each step gives the query in two forms. Refer to
[the naming rules](README.md#the-names-on-your-backend) for which form your
backend needs, and for the reasons a query can return nothing.

---

## Step 1. Confirm that the resume is the cause

Read the two numbers together. The top line is what the user paid. The bottom
line is what the node used.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le) (
  rate(atenet_router_route_duration_seconds_bucket{
        ate_router_resume="triggered"}[5m])))

histogram_quantile(0.95, sum by (le) (
  rate(ate_actor_restore_duration_seconds_bucket{
        ate_snapshot_phase="total"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le) (
  rate({__name__="atenet.router.route.duration_bucket",
        "ate.router.resume"="triggered"}[5m])))

histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.actor.restore.duration_bucket",
        "ate.snapshot.phase"="total"}[5m])))
```

**The restore histogram holds the failed restores too.** It has no key that
marks a failure. A phase that fails holds its timer until it gives up, thus
failures raise this number without any resume becoming slower. Read step 6
before you trust it.

* The two numbers agree — the node is the cause. Go to step 2.
* The router number is much larger — the time went to the queue or to the
  scheduler. Go to step 5.
* The `unknown` series of the router increases — the resumes fail. Go to
  step 6 first.
* The restore query is empty but resumes occur — the resumes are boots. Confirm
  it with the query below.

**A quantile at the last bucket is saturated.** The two instruments do not use
the same buckets, and the router histogram ends before the restore histogram
of atelet. A value at or near the end of either range means only that the true
value is somewhere above the buckets, thus the two cannot be compared there.
Read the mean instead, as step 2 does.

**Prometheus**

```promql
sum by (ate_snapshot_kind) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.snapshot.kind") (
  rate({__name__="ate.actor.lifecycle.operation.duration_count",
        "ate.actor.operation.name"="resume"}[5m]))
```

## Step 2. Find the phase

**Know the failure rate before you read the time.** A phase that fails holds
its timer until it gives up. Thus a few failures make the phase look slow. If
step 6 shows failures, the tail of this step is the failures, not slow
restores.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, ate_snapshot_phase) (
  rate(ate_actor_restore_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "ate.snapshot.phase") (
  rate({__name__="ate.actor.restore.duration_bucket"}[5m])))
```

**Read the mean as well.** These buckets are wide at the tail, thus a quantile
in the last bucket is an interpolation that can be far above the true value:

**Prometheus**

```promql
sum by (ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_sum[30m]))
/ sum by (ate_snapshot_phase) (
  increase(ate_actor_restore_duration_seconds_count[30m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.snapshot.phase") (
  increase({__name__="ate.actor.restore.duration_sum"}[30m]))
/ sum by("ate.snapshot.phase") (
  increase({__name__="ate.actor.restore.duration_count"}[30m]))
```

The `registry.ate.snapshot` group of
[the registry](../metrics/registry/metrics.yaml) says what each phase covers.
The slowest phase says where to go next:

| Phase | Go to |
|---|---|
| `volume_mount` | The logs of atelet. |
| `manifest_fetch`, `download` | Step 3. |
| `oci_unpack`, `sandbox_assets` | Step 4. |
| `ateom_restore` | The logs of ateom. |

**Compare the count of each phase with the count of `total`.** atelet does not
record a phase that did not start. Thus a phase with fewer samples than the
phases before it shows where the restores stop. For example, if `download` has
as many samples as `total` but `ateom_restore` has fewer, the restores reached
the sandbox runtime and died there. Go to step 6. Make this comparison for one
`ate.snapshot.kind` at a time, because a `local` restore has no download.

## Step 3. Examine the snapshot and the storage

A large snapshot makes a long download. Compare the templates.

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, ate_template_name, file_name) (
  rate(atelet_snapshot_size_bytes_bucket[1h])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, "ate.template.name", "file.name") (
  rate({__name__="atelet.snapshot.size_bucket"}[1h])))
```

atelet records one measurement for each image file, not one for each
checkpoint. Use `file.name` to compare the same type of image.

```bash
kubectl ate get actor-snapshots -a <atespace>
```

If the size did not change but the download did, the storage backend is the
cause. Read step 6 for the failures.

## Step 4. Examine the image cache on the node

A miss adds a pull and an unpack to each resume.

**Prometheus**

```promql
sum by (ate_imagecache_outcome) (
  rate(ate_imagecache_requests_total[5m]))

sum by (error_type) (
  rate(ate_imagecache_requests_total{
        ate_imagecache_outcome="error"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.imagecache.outcome") (
  rate({__name__="ate.imagecache.requests"}[5m]))

sum by("error.type") (
  rate({__name__="ate.imagecache.requests",
        "ate.imagecache.outcome"="error"}[5m]))
```

Calculate the hit ratio as `hit / (hit + miss)`. Keep the outcomes that are not
a lookup result out of the denominator. The
`registry.ate.imagecache` group of
[the registry](../metrics/registry/metrics.yaml) lists them.

Only the `error` outcome carries `error.type`, which holds the HTTP status that
the registry of the image returned. Group by it to divide a credential fault
from a rate limit from a fault of the registry. The permitted values are on
`metric.ate.imagecache.requests` in the same file.

## Step 5. Examine the control plane

Use this step only if step 1 sent you here.

**Prometheus**

```promql
sum by (ate_scheduler_outcome) (
  rate(ate_scheduler_assignment_duration_seconds_count[5m]))

histogram_quantile(0.95, sum by (le) (
  rate(ate_scheduler_assignment_duration_seconds_bucket{
        ate_scheduler_outcome="assigned"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome") (
  rate({__name__="ate.scheduler.assignment.duration_count"}[5m]))

histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.scheduler.assignment.duration_bucket",
        "ate.scheduler.outcome"="assigned"}[5m])))
```

* The outcome is `no_capacity` — this is a capacity fault, not a resume
  fault. Read [capacity-is-full.md](capacity-is-full.md).
* The outcome is `assigned` but the time is large — a delay in the store. The
  store has no metrics. Read the logs of ateapi.

The user also pays the parking time with the resume time:

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le, outcome) (
  rate(atenet_router_parking_wait_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le, outcome) (
  rate({__name__="atenet.router.parking.wait.duration_bucket"}[5m])))
```

## Step 6. Find out if the resumes fail

A failed resume is not the same fault as a slow resume, but a failure also
makes the phase look slow, thus read this step together with step 2. The
restore histogram of atelet does not mark a failure. The lifecycle histogram
of ateapi does: `error.type` is present only on an operation that failed.

**Prometheus**

```promql
sum by (ate_template_name, error_type) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume", error_type!=""}[5m]))
/ ignoring(error_type) group_left
sum by (ate_template_name) (
  rate(ate_actor_lifecycle_operation_duration_seconds_count{
        ate_actor_operation_name="resume"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.template.name", "error.type") (
  rate({__name__="ate.actor.lifecycle.operation.duration_count",
        "ate.actor.operation.name"="resume", "error.type"!=""}[5m]))
/ ignoring("error.type") group_left
sum by("ate.template.name") (
  rate({__name__="ate.actor.lifecycle.operation.duration_count",
        "ate.actor.operation.name"="resume"}[5m]))
```

The result is the fraction of the resumes of each template that failed, for
each gRPC status code. The code tells you the class of the failure, not the
component. Use step 2 to find the phase where the restores stop, and then read
the logs of that component:

| Phase where the restores stop | Examine |
|---|---|
| `manifest_fetch`, `download` | The storage backend, and the URL of the snapshot object. The logs of atelet. |
| `volume_mount`, `oci_unpack`, `sandbox_assets` | The node. The logs of atelet. |
| `ateom_restore` | The sandbox runtime. The logs of ateom. |
| No phase, only `total` or nothing | The failure occurred before the work reached the node. The logs of ateapi. |

A slow resume and a failed suspend are related. A suspend that fails leaves no
good snapshot for the next resume. Read the crash counter and the checkpoint
phases:

**Prometheus**

```promql
sum by (ate_actor_operation_name, ate_template_name) (
  rate(ate_actor_crashes_total[5m]))

histogram_quantile(0.95, sum by (le, ate_snapshot_phase) (
  rate(ate_actor_checkpoint_duration_seconds_bucket[5m])))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.actor.operation.name", "ate.template.name") (
  rate({__name__="ate.actor.crashes"}[5m]))

histogram_quantile(0.95, sum by(le, "ate.snapshot.phase") (
  rate({__name__="ate.actor.checkpoint.duration_bucket"}[5m])))
```

`ate.actor.operation.name` on the crash counter tells you which operation lost
the actor. The `ate.actor.crashed` event in
[`events.yaml`](../metrics/registry/events.yaml) names the actor, which no
metric label may.

## Step 7. Examine the load on the node

A node with no free memory or no free CPU makes each resume slow. One worker
holds many actors, thus a resume shares the CPU and the memory of its worker
with the actors that are already there. A busy actor makes the resume of its
neighbors slow. The scheduler does not prevent this: it compares the limits of
the actors with the capacity of the worker, not what the actors use.

**Prometheus**

```promql
sum by (ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_template_name, ate_stats_source) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))

ate_actor_stats_sampled_actors
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.template.name", "ate.stats.source") (
  {__name__="ate.actor.stats.memory.working_set"})

sum by("ate.template.name", "ate.stats.source") (
  rate({__name__="ate.actor.stats.cpu.time"}[5m]))

{__name__="ate.actor.stats.sampled_actors"}
```

Group the data by `ate.stats.source`. Do not add the sources together. The
`cgroup` source includes the load of the sandbox runtime. The `guest-agent`
source includes only the containers of the workload.

**Add these across the nodes with `sum`.** Each atelet measures only the actors
on its own node, thus one series for each node, and the sum is the fleet. This
is the opposite of `ate.workerpool.workers`, where each ateapi reports the whole
fleet and a sum multiplies it. Read the emitter before you choose the operator:

| Metric | Emitter | Each series covers | Operator |
|---|---|---|---|
| `ate.actor.stats.*` | atelet, one for each node | One node | `sum` |
| `ate.workerpool.workers` | ateapi, some replicas | The whole fleet | `max` |

The pool keys are absent when the atelet DaemonSet does not set `NODE_NAME`
through the Downward API. The samples still flow; they carry no
`ate.workerpool.*`.

Use `working_set` against a memory limit. The `usage` value includes the page
cache that the node can release. `sampled_actors` is the denominator: if it
decreases but the number of actors does not, the sweep cannot measure some
actors.

```bash
kubectl ate top workers
kubectl logs -n ate-system <atelet-pod>
```

---

## The blind spots of this scenario

| Area | Effect on a slow resume |
|---|---|
| The store in ateapi | A delay looks like unmeasured time in the lifecycle and the assignment histograms. |
| The worker cache in ateapi | An old view of the fleet gives a worker that is not in operation. This looks like a resume failure with no cause. |
| The eviction of the image cache | You see the hits and the misses, but not the disk pressure from the layer pool. |
| The build of a golden image | Nothing measures it. A slow first activation of a new template has no data. |

## Move from a template to an actor

No resume metric has the name, the UID, or the atespace of an actor. The
cardinality rules forbid these keys. When the metrics give you a slow template,
use the logs and the traces to find the actor:

```bash
kubectl ate get actors -a <atespace>
kubectl ate logs actors <actor-name> -a <atespace> -f
kubectl ate resume actor <actor-name> -a <atespace> --trace
```

The `--trace` flag prints a trace ID. Put it in Cloud Trace or Jaeger to see
each step of the one resume.
