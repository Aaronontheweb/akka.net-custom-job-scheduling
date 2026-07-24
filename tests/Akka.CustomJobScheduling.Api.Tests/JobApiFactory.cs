using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace Akka.CustomJobScheduling.Api.Tests;

/// <summary>
/// Runs the real API host in <c>LocalTest</c> mode: no Redis, no remoting, no cluster — but the
/// same endpoints, the same DI wiring, and a complete in-process scheduler with two fake worker
/// nodes.
/// </summary>
/// <remarks>
/// These tests exist because two defects reached a running cluster before anything caught them:
/// a <c>DELETE</c> handler with an inferred body, which fails at route-build time and 500s
/// <i>every</i> endpoint; and a heartbeat built on <c>PeriodicTimer</c>, which throws once a
/// second tick is requested while the first is still pending. Neither is reachable from an actor
/// test — both die on the first HTTP request.
/// </remarks>
public sealed class JobApiFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("Akka:ExecutionMode", "LocalTest");
        builder.UseSetting("Jobs:LocalNodes:0", "node-a");
        builder.UseSetting("Jobs:LocalNodes:1", "node-b");

        builder.ConfigureAppConfiguration(config =>
            config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Akka:ExecutionMode"] = "LocalTest"
            }));
    }
}
