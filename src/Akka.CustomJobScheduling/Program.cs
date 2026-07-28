using Aaron.Akka.Aspire;
using Aaron.Akka.Discovery.Redis;
using Akka.Cluster.Hosting;
using Akka.CustomJobScheduling;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Serialization;
using Akka.Hosting;
using Akka.Persistence.Redis.Hosting;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
});

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation())
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation())
    .UseOtlpExporter();

// How many work units this node can run in parallel. The scheduler assumes uniform capacity across
// the cluster: the tracker applies its own configured value to every worker, so set this the same on
// every replica. Heterogeneous nodes would need each to advertise its own capacity, which this
// system deliberately doesn't do.
var nodeCapacity = builder.Configuration.GetValue(
    "Jobs:NodeCapacity",
    JobSchedulingHostingExtensions.DefaultNodeCapacity);

builder.Services.AddJobSchedulingServices(AkkaExecutionMode.Clustered, nodeCapacity);

builder.Services.AddAkka("CustomJobScheduling", (akkaBuilder, serviceProvider) =>
{
    var configuration = serviceProvider.GetRequiredService<IConfiguration>();
    var redisConnection = configuration.GetConnectionString("akka-discovery")
        ?? throw new InvalidOperationException("The shared Redis connection string is required.");

    akkaBuilder.ConfigureLoggers(setup =>
    {
        setup.ClearLoggers();
        setup.AddLoggerFactory();
    });

    akkaBuilder.WithAspireClusterBootstrap(serviceProvider,
        configureDiscovery: (hosting, clusterConfiguration) =>
        {
            hosting.WithRedisDiscovery(
                redisConnection,
                clusterConfiguration["Akka:Cluster:ServiceName"]);
        },
        clusterConfigure: cluster => cluster.Roles = ["worker"]);

    akkaBuilder
        .WithAkkaClusterReadinessCheck()
        .WithRedisPersistence(
            journalOptions: new RedisJournalOptions
            {
                ConfigurationString = redisConnection,
                KeyPrefix = "job-scheduling:journal"
            },
            snapshotOptions: new RedisSnapshotOptions
            {
                ConfigurationString = redisConnection,
                KeyPrefix = "job-scheduling:snapshots"
            },
            journalBuilder: journal => journal
                .WithHealthCheck(
                    HealthStatus.Unhealthy,
                    tags: ["ready", "persistence", "redis", "journal"])
                .WithConnectivityCheck(
                    tags: ["ready", "persistence", "redis", "journal", "connectivity"]),
            snapshotBuilder: snapshots => snapshots
                .WithHealthCheck(
                    HealthStatus.Unhealthy,
                    tags: ["ready", "persistence", "redis", "snapshot-store"])
                .WithConnectivityCheck(
                    tags: ["ready", "persistence", "redis", "snapshot-store", "connectivity"]));

    akkaBuilder
        .AddJobSchedulingSerializer()
        .WithJobSchedulingActors(AkkaExecutionMode.Clustered);

    if (configuration.GetValue(JobLoadGenerator.EnabledKey, false))
        akkaBuilder.WithSyntheticJobTraffic();
});

builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapHealthChecks("/healthz");
app.MapHealthChecks("/healthz/live", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("liveness")
});
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});
app.MapGet("/", () => Results.Ok(new { Service = "Akka.CustomJobScheduling" }));

app.Run();
