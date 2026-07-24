# Akka.NET Custom Job Scheduling

An Aspire-hosted Akka.NET cluster prepared for demonstrating custom scheduling of long-running jobs.

The repository currently contains infrastructure only. The job coordinator, persistence model, worker protocol, and scheduling behavior will be added once their requirements are defined.

## Projects

- `Akka.CustomJobScheduling.AppHost` provisions Redis discovery and starts three service replicas.
- `Akka.CustomJobScheduling` joins the Akka.NET cluster through the Aspire plugin and exposes health endpoints.

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
