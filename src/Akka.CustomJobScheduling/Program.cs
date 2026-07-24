using Aaron.Akka.Aspire;
using Aaron.Akka.Discovery.Redis;
using Akka.Hosting;
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

builder.Services.AddAkka("CustomJobScheduling", (akkaBuilder, serviceProvider) =>
{
    akkaBuilder.ConfigureLoggers(setup =>
    {
        setup.ClearLoggers();
        setup.AddLoggerFactory();
    });

    akkaBuilder.WithAspireClusterBootstrap(serviceProvider,
        configureDiscovery: (hosting, configuration) =>
        {
            var redisConnection = configuration.GetConnectionString("akka-discovery");
            if (!string.IsNullOrWhiteSpace(redisConnection))
            {
                hosting.WithRedisDiscovery(
                    redisConnection,
                    configuration["Akka:Cluster:ServiceName"]);
            }
        },
        clusterConfigure: cluster => cluster.Roles = ["worker"]);
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
    Predicate = check => check.Tags.Contains("readiness")
});
app.MapGet("/", () => Results.Ok(new { Service = "Akka.CustomJobScheduling" }));

app.Run();
