# Akka.NET Custom Job Scheduling

An Aspire-hosted Akka.NET cluster demonstrating custom scheduling of long-running jobs across worker nodes.

Jobs are submitted with a size in work units. A cluster singleton tracks every node's capacity, places each job on the eligible node with the most room, and requeues work when a node leaves the cluster. Progress flows back to whoever submitted the job.

## Projects

- `Akka.CustomJobScheduling.AppHost` provisions Redis and starts the worker replicas and the API.
- `Akka.CustomJobScheduling` is a worker: it joins under the `worker` role, hosts the tracker singleton and a job receiver, and configures Redis-backed Akka.Persistence.
- `Akka.CustomJobScheduling.Api` is the HTTP surface. It joins under the `api` role and runs no jobs — it reaches the tracker singleton and the submitter shard region through proxies.
- `Akka.CustomJobScheduling.Core` holds the domain model, the scheduling state machine, and the actors.

Only nodes carrying the `worker` role count as capacity. The API joins the same cluster but hosts no receiver, so the tracker must not place work there.

The sample intentionally shares one Redis resource for cluster discovery, the persistence journal, and snapshot storage. Separate key prefixes isolate persisted job state from discovery records.

## Design

State is separated from the actors that own it. `JobTrackerState` is a pure record with two entry points — `Decide(command, now)` turns a command into the events it implies, and `Apply(event)` folds one event into a new state. It holds no `IActorRef`, never reads the clock, and contains every scheduling rule: validation, placement, capacity accounting, and requeue-on-node-loss.

`JobTrackerActor` is the shell around it: read the clock, decide, fold, reply, dispatch. That split is what lets the scheduling rules be tested as plain functions.

Messages are grouped by their role in the effects system:

| Kind | Meaning |
| --- | --- |
| Commands | Requests that get validated and may be rejected |
| Events | Facts that already happened; the persisted schema |
| Queries | Read-only, including subscriptions |
| Notifications | The public contract pushed to subscribers and submitters |

### Actors

```
JobTrackerActor    cluster singleton; owns JobTrackerState
JobReceiverActor   one per node; entry point for work
JobExecutorActor   one per running job; stops when done
JobSubmitterActor  shard entity per submitter; receives progress
```

## Dashboard

The API serves a dashboard at `/` — cluster capacity, per-node utilisation bars, a live job table, and a submit box. It's a single static file with no build step and no framework, driven entirely by the `/cluster/events` stream: one snapshot on connect, then a frame per transition. Nothing polls.

## API

| | | |
| --- | --- | --- |
| `POST` | `/jobs` | `{"id":"...","size":25,"submitterId":"..."}` → `202` + `Location` |
| `GET` | `/jobs` | All jobs; `?includeFinished=false&limit=50` |
| `GET` | `/jobs/{id}` | Current status |
| `GET` | `/jobs/{id}/events` | Server-sent events until that job finishes |
| `DELETE` | `/jobs/{id}?submitterId=...&reason=...` | Cancel |
| `GET` | `/cluster/queue` | Queue depth and per-node capacity |
| `GET` | `/cluster/events` | Server-sent events for the whole queue |

Rejections map onto status codes directly: duplicate id `409`, unknown job `404`, already finished `409`, wrong submitter `403`, larger than any node `422`.

### Streaming

An HTTP response isn't an `IActorRef`, so a `JobStreamSupervisor` spawns one short-lived bridge actor per connection. The bridge subscribes to the tracker, writes each push into a bounded channel, and completes the channel when the job reaches a terminal state — so the handler's `await foreach` ends on its own.

The channel drops oldest when a client is slow. That's safe *only* because every notification carries absolute progress rather than a delta: a reader that misses 40% → 50% is still correct once it sees 60%.

Submissions route through the submitter shard region rather than straight at the tracker, so the submitter entity stays re-acquirable by id after a recovery.

## Persistence

`JobTrackerActor` is a `ReceivePersistentActor` on a fixed persistence id (`job-tracker`) — fixed because the singleton must resume the same stream after failing over to a different host. Commands are validated, the resulting events are persisted, and only then does the actor fold them in, reply, and dispatch. Nothing is ever handed to a worker that hasn't been recorded first.

Snapshots every 200 events bound replay time; the journal remains the source of truth.

### Reconciling on recovery

Replay is faithful, which is the problem: a node that left while the tracker was down still appears in the restored map, and no `MemberRemoved` is ever coming for it. So on subscribe the membership source reports the *whole* current member set as `SyncNodes`, and the tracker reconciles — dropping nodes that are gone and requeueing their work before placing anything new. The local membership fake mirrors that exactly, which is what makes the failover path testable.

## Serialization

`JobSchedulingSerializer` is a `SerializerWithStringManifest` over MessagePack. Manifests are short, stable strings (`e:job-accepted`, `c:submit-job`) rather than type names, so classes can be renamed or moved between namespaces without orphaning events already on disk.

Payloads are written by hand rather than through a reflection-based resolver: the domain records stay free of serialization attributes, and the wire schema is something you can read. Every composite is framed as an array whose readers tolerate extra trailing fields, giving one evolution rule — **append at the end, never reorder or remove**.

`JobStreamMessages` is deliberately *not* registered. It carries a `ChannelReader` and a child `IActorRef`, is in-process by construction, and leaving it unregistered means an accidental remote send fails loudly instead of quietly.

### Losing a node

Unreachable nodes stop receiving new work but keep what they already hold. Requeueing waits for `MemberRemoved`, because a node on the far side of a healing partition is still running its jobs — moving them on unreachability alone would run the same job twice.

## Testing

Two suites, split on purpose:

- `Akka.CustomJobScheduling.Core.Tests` exercises the scheduling rules with no `ActorSystem`, no `TestKit`, and no `await`.
- `Akka.CustomJobScheduling.Actors.Tests` exercises the actors. Cluster membership sits behind `IClusterMembershipSource`, so a test stands up several worker nodes, downs one mid-job, and asserts the work migrates — all in one process, without remoting. A smaller clustered-mode suite runs the same registration code against a real single-node cluster to keep both branches honest.

```powershell
dotnet test Akka.CustomJobScheduling.slnx
```

## Health Checks

- `/healthz` reports every registered check.
- `/healthz/live` reports ActorSystem liveness.
- `/healthz/ready` reports cluster readiness, persistence plugin health, and Redis connectivity.

## Prerequisites

- .NET 10 SDK, as selected by `global.json`
- A Docker-compatible container runtime for the Redis resource

## Run

```powershell
dotnet run --project src/Akka.CustomJobScheduling.AppHost
```

The Aspire dashboard displays the Redis resource, service replicas, logs, traces, metrics, and health state.

## Build

```powershell
dotnet build Akka.CustomJobScheduling.slnx
```
