using Aaron.Akka.Aspire;
using Aaron.Akka.Discovery.Redis;
using Akka.Cluster.Hosting;
using Akka.CustomJobScheduling.Api.Endpoints;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Serialization;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
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

// Clustered by default so local development exercises the same path production does. Setting
// Akka:ExecutionMode=LocalTest makes this host self-contained — scheduler, fake worker nodes and
// all — with no Redis and no remoting, which is what integration tests run against.
var executionMode = builder.Configuration.GetValue("Akka:ExecutionMode", AkkaExecutionMode.Clustered);

builder.Services.AddJobSchedulingServices(executionMode);

builder.Services.AddAkka("CustomJobScheduling", (akkaBuilder, serviceProvider) =>
{
    akkaBuilder.ConfigureLoggers(setup =>
    {
        setup.ClearLoggers();
        setup.AddLoggerFactory();
    });

    if (executionMode == AkkaExecutionMode.LocalTest)
    {
        var localNodes = builder.Configuration
            .GetSection("Jobs:LocalNodes").Get<string[]>() ?? ["node-a", "node-b"];

        akkaBuilder
            .AddJobSchedulingSerializer()
            .WithInMemoryJournal()
            .WithInMemorySnapshotStore()
            .WithJobSchedulingActors(AkkaExecutionMode.LocalTest, localNodes)
            .WithAnnouncedLocalNodes(localNodes)
            .WithJobStreams();

        return;
    }

    var configuration = serviceProvider.GetRequiredService<IConfiguration>();
    var redisConnection = configuration.GetConnectionString("akka-discovery")
        ?? throw new InvalidOperationException("The shared Redis connection string is required.");

    // The API hosts no work: it joins the cluster only to reach the tracker singleton and the
    // submitter shard region, both of which live on "worker" nodes.
    akkaBuilder.WithAspireClusterBootstrap(serviceProvider,
        configureDiscovery: (hosting, clusterConfiguration) =>
        {
            hosting.WithRedisDiscovery(
                redisConnection,
                clusterConfiguration["Akka:Cluster:ServiceName"]);
        },
        clusterConfigure: cluster => cluster.Roles = ["api"]);

    akkaBuilder
        .AddJobSchedulingSerializer()
        .WithAkkaClusterReadinessCheck()
        .WithJobSchedulingClient(AkkaExecutionMode.Clustered);
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

// Serves wwwroot/index.html, the dashboard.
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api", () => Results.Ok(new { Service = "Akka.CustomJobScheduling.Api" }));
app.MapJobEndpoints();

app.Run();

/// <summary>
/// Exposed so integration tests can drive this host through <c>WebApplicationFactory</c>.
/// </summary>
public partial class Program;
