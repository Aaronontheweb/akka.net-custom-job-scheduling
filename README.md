# Akka.NET Custom Job Scheduling

An Aspire-hosted Akka.NET cluster demonstrating custom scheduling of long-running jobs across worker nodes.

Jobs are submitted with a size in work units. A cluster singleton tracks every node's capacity, places each job on the eligible node with the most room, and requeues work when a node leaves the cluster. Progress flows back to whoever submitted the job.

## Projects

- `Akka.CustomJobScheduling.AppHost` provisions Redis and starts three service replicas.
- `Akka.CustomJobScheduling` joins the Akka.NET cluster through the Aspire plugin, configures Redis-backed Akka.Persistence, and generates synthetic job traffic.
- `Akka.CustomJobScheduling.Core` holds the domain model, the scheduling state machine, and the actors.

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
