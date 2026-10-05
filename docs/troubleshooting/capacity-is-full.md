# Substrate has no capacity for an actor

## Context

Substrate puts many actors on few workers. A worker is a pod in a WorkerPool.
One worker holds more than one actor. Each worker reports a capacity: a number
of actor slots, and CPU and memory. An actor must get a slot and enough CPU and
memory on one worker to run. When no worker has room, a new resume cannot
start. The router parks the request, and then it returns
`503 no free workers available`.

The scheduler gives a new actor to the less-loaded worker of two random workers
that have room. Thus the actors spread across the pool, and most workers hold
some actors before any worker is full.

Four different states look the same at the edge:

| State | What it means | Where to see it |
|---|---|---|
| The slots are full | Each worker that can take actors has no free slot. | `ate.workerpool.workers`, states `idle` and `partial` = 0 |
| The CPU or the memory is full | Workers have free slots, but not enough CPU or memory for the next actor. | `partial` > 0, and the scheduler reports `no_capacity` |
| The pool did not grow | Kubernetes did not give the pods that the pool asked for. | `desired_workers` − `ready_workers` > 0 |
| The workers are hidden | Workers have room, but a constraint removes them. | No metric. Refer to step 3. |

The last two states with free slots are the ones that confuse people.
`ate.workerpool.workers` shows workers with free slots and the resumes still
fail.

Two pool keys identify a pool together: `ate.workerpool.namespace` and
`ate.workerpool.name`. A WorkerPool has a namespace, thus the name alone merges
the pools of different namespaces into one series.

Each step gives the query in two forms. Refer to
[the naming rules](README.md#the-names-on-your-backend) for which form your
backend needs, and for the reasons a query can return nothing.

---

## Step 1. Compare the demand with the supply

**Prometheus**

```promql
max by (ate_workerpool_namespace, ate_workerpool_name, ate_sandbox_class,
        ate_worker_state) (
  ate_workerpool_workers)
```

**Cloud Monitoring / GMP**

```promql
max by("ate.workerpool.namespace", "ate.workerpool.name", "ate.sandbox.class",
       "ate.worker.state") (
  {__name__="ate.workerpool.workers"})
```

**Use `max` and not `sum` across the replicas of ateapi.** Each ateapi replica
reports the whole fleet, thus a sum multiplies the fleet by the number of
replicas. Two replicas make 10 idle workers read as 20. The `instance` label
divides the replicas:

**Prometheus**

```promql
sum by (instance, ate_worker_state) (ate_workerpool_workers)
```

**Cloud Monitoring / GMP**

```promql
sum by(instance, "ate.worker.state") ({__name__="ate.workerpool.workers"})
```

**Keep `ate.sandbox.class` in the result.** The scheduler gives an actor only a
worker of the same sandbox class. A worker with the class `unknown` takes no
actor. A sum across the classes adds that worker to the capacity of the pool.

After the `max`, you can add the counts together. The sum across the states is
the size of the pool. The sum across the pools is the size of the fleet.

The `registry.ate.workerpool` group of
[the registry](../metrics/registry/metrics.yaml) says what each value of
`ate.worker.state` is.

* `idle` = 0 and `partial` = 0 — each slot is full. Go to step 2.
* `idle` or `partial` > 0 and the resumes still fail — the CPU or the memory
  is full, or something removes the workers that have room. Go to step 3.
* `unschedulable` > 0 — the scheduler cannot use these workers. A worker that
  drains, and a worker that has not reported its capacity yet, have this
  state. A rollout makes many of them:

  ```bash
  kubectl ate get workers
  kubectl rollout status deploy -n <workerpool-namespace> <workerpool-name>
  ```

  A long pod grace period keeps a draining worker in the count for as long as
  the period lasts.

**The states count workers, not slots.** A worker with 1 slot in use and a
worker with all slots but one in use are both `partial`. Thus this instrument
does not tell you how full the pool is. It tells you only whether a worker
with a free slot exists.

## Step 2. Find out if the new capacity arrived

**Prometheus**

```promql
max by (ate_workerpool_namespace, ate_workerpool_name) (
  ate_workerpool_desired_workers)
- max by (ate_workerpool_namespace, ate_workerpool_name) (
  ate_workerpool_ready_workers)
```

**Cloud Monitoring / GMP**

```promql
max by("ate.workerpool.namespace", "ate.workerpool.name") (
  {__name__="ate.workerpool.desired_workers"})
- max by("ate.workerpool.namespace", "ate.workerpool.name") (
  {__name__="ate.workerpool.ready_workers"})
```

Reduce each side with `max` before the subtraction. A bare `on()` join needs
exactly one series for each pool on both sides, and it fails with
`found duplicate series for the match group` as soon as a second replica or a
second `instance` reports the same pool.

A value above 0 for more than a few minutes means that Kubernetes did not give
the pods. The usual causes are an empty node pool, a quota, or a worker pod
that cannot start.

```bash
kubectl ate get workers
kubectl get pods -n <workerpool-namespace> -o wide
kubectl describe pod -n <workerpool-namespace> <worker-pod>
kubectl get events -n <workerpool-namespace> --sort-by=.lastTimestamp | tail -20
```

If the difference is 0, the pool has each pod that it asked for, thus
Kubernetes is not the subject. Before you make `spec.replicas` larger, read
step 6: a pool can be full of actors that do no work, and the answer is then a
suspend policy and not more workers. Grow the pool when step 6 shows real load.

## Step 3. Find out why the workers with room take no actor

The scheduler keeps a worker only if all of these are true:

* The sandbox class of the worker is the same as the class of the template.
* The worker is active.
* The labels of the worker match the `workerSelector` of the template and of
  the actor.
* The worker is on a node that holds the local snapshot of the actor, if the
  actor has one.
* The worker has a free slot, and enough CPU and memory for the limits of the
  actor.

No metric tells you which condition removed the workers. The selectors and the
nodes are barred from a metric label. Read the `no_capacity` outcome by
sandbox class first:

**Prometheus**

```promql
sum by (ate_sandbox_class) (
  rate(ate_scheduler_assignment_duration_seconds_count{
        ate_scheduler_outcome="no_capacity"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.sandbox.class") (
  rate({__name__="ate.scheduler.assignment.duration_count",
        "ate.scheduler.outcome"="no_capacity"}[5m]))
```

The `no_capacity` outcome names no pool. Compare its sandbox class with the
classes of step 1:

| Workers with room in step 1, same class | Cause |
|---|---|
| None | The pool of that class is full. Go to step 2. |
| None, but another class has room | The template asks for a class that has no free worker. Examine the sandbox class of the template and of the pools. |
| Some | The CPU or the memory, a selector, or a node pin removes them. Use the table below. |

When workers of the correct class have room, compare the actor with the
workers:

```bash
kubectl ate get workers
kubectl ate top workers
kubectl ate get actors -A
```

| What you see | Cause |
|---|---|
| The workers with free slots have little free CPU or memory | The actors on each worker use the resources before they use the slots. Make the limits of the template smaller, give the workers more resources, or add workers. |
| The labels of the workers with room do not match the selector | A label selector hides them. Examine the selector of the actor and of the template, and the labels of the workers. |
| The actor was paused, and its node has no worker with room | The actor is pinned to a node. Refer to the note below. |

**Node pinning is a hard filter.** A pause writes a snapshot on the node VM,
and the resume that follows accepts only a worker on that VM. It has no
fallback: the scheduler does not place a pinned actor on a different node.
Suspend the actor rather than pause it when it must be free to move, or add
capacity to the node that holds the snapshot.

## Step 4. Read the decision of the scheduler

**Prometheus**

```promql
sum by (ate_scheduler_outcome) (
  rate(ate_scheduler_assignment_duration_seconds_count[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome") (
  rate({__name__="ate.scheduler.assignment.duration_count"}[5m]))
```

| Outcome | Go to |
|---|---|
| `assigned` | The scheduler took a worker. If the users still get a 503 error, go to step 5. |
| `no_capacity` | This is capacity pressure and not a failure. Go to step 2, step 3 and step 6. |
| `error` | The query below. Only this outcome has an `error.type` key. |

**Prometheus**

```promql
sum by (ate_scheduler_outcome, error_type) (
  rate(ate_scheduler_assignment_duration_seconds_count{
        ate_scheduler_outcome="error"}[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.scheduler.outcome", "error.type") (
  rate({__name__="ate.scheduler.assignment.duration_count",
        "ate.scheduler.outcome"="error"}[5m]))
```

The scheduler can also be slow and not full:

**Prometheus**

```promql
histogram_quantile(0.95, sum by (le) (
  rate(ate_scheduler_assignment_duration_seconds_bucket{
        ate_scheduler_outcome="assigned"}[5m])))
```

**Cloud Monitoring / GMP**

```promql
histogram_quantile(0.95, sum by(le) (
  rate({__name__="ate.scheduler.assignment.duration_bucket",
        "ate.scheduler.outcome"="assigned"}[5m])))
```

This step is a read of the cache and some writes to the store. A large value
means a delay in the store. The store has no metrics. Read the logs of ateapi.

## Step 5. Measure the cost at the edge

The steps above measure the fleet. This step measures what the users pay.

**Prometheus**

```promql
sum by (ate_router_outcome) (
  rate(atenet_router_route_duration_seconds_count[5m]))

atenet_router_parking_active

sum(rate(atenet_router_parking_rejected_total[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.router.outcome") (
  rate({__name__="atenet.router.route.duration_count"}[5m]))

{__name__="atenet.router.parking.active"}

sum(rate({__name__="atenet.router.parking.rejected"}[5m]))
```

* `no_capacity` on the router — the park budget ended and no worker had room.
  The user got a 503 error.
* `parking.rejected` above zero — the parking area is full. The router sheds
  load before it tries a resume, and it reports `unavailable`, not
  `no_capacity`.

Refer to [requests-are-slow.md](requests-are-slow.md) for the full path.

## Step 6. Find out if the pressure is real

A pool that is full of actors that do no work is a different fault. Each actor
on a worker holds a slot and its CPU and memory limits, also when it is idle.
Read the resource use of the actors:

**Prometheus**

```promql
sum by (ate_template_name, ate_stats_source) (
  ate_actor_stats_memory_working_set_bytes)

sum by (ate_template_name, ate_stats_source) (
  rate(ate_actor_stats_cpu_time_seconds_total[5m]))
```

**Cloud Monitoring / GMP**

```promql
sum by("ate.template.name", "ate.stats.source") (
  {__name__="ate.actor.stats.memory.working_set"})

sum by("ate.template.name", "ate.stats.source") (
  rate({__name__="ate.actor.stats.cpu.time"}[5m]))
```

Group the data by `ate.stats.source`. Do not add the sources together. The
`cgroup` source includes the load of the sandbox runtime. The `guest-agent`
source includes only the containers of the workload.

```bash
kubectl ate top workers
kubectl ate get actors -A
```

If the workers hold actors that are idle, the suspend policy is the subject,
not the size of the pool. If the actors use much less than their limits, the
limits of the template are the subject: they hold resources that the actors
do not use.

---

## The blind spots of this scenario

| Area | Effect |
|---|---|
| The worker cache in ateapi | If the view of the fleet is old, the scheduler gives a worker that is not in operation. This looks like a resume failure with no cause in the scheduler data. |
| The actor population | Nothing counts the actors by state. These metrics count the operations, not the actors. |
| The use of the slots | `ate.workerpool.workers` counts workers by state, not slots. No metric gives the number of slots in use in a pool. |
| The constraint that removed a worker | No metric names the selector or the node that removed a worker. |
| The store in ateapi | A delay looks like unmeasured time in the assignment histogram. |
