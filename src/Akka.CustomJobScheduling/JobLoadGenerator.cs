using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Hosting;

namespace Akka.CustomJobScheduling;

/// <summary>
/// Manufactures synthetic job traffic so the demo has something to schedule.
/// </summary>
/// <remarks>
/// Deliberately not an actor and deliberately untested — in a real system this traffic would arrive
/// from a web API. It's a scheduled closure hung off an Akka.Hosting startup block, which is the
/// least ceremony that produces a continuous workload.
/// </remarks>
public static class JobLoadGenerator
{
    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan SubmitInterval = TimeSpan.FromSeconds(4);

    private const int SubmitterCount = 5;
    private const uint MinJobSize = 5;
    private const uint MaxJobSize = 45;

    /// <summary>
    /// Configuration flag. Off by default now that the API can submit real work; turn it on to
    /// watch the scheduler move without driving traffic by hand.
    /// </summary>
    public const string EnabledKey = "Jobs:LoadGenerator:Enabled";

    public static AkkaConfigurationBuilder WithSyntheticJobTraffic(
        this AkkaConfigurationBuilder builder) =>
        builder.AddStartup((system, registry) =>
        {
            var submitters = registry.Get<JobSubmitterManagerKey>();

            // Each node generates its own traffic; the node's own address keeps job ids unique
            // across replicas without any coordination.
            var origin = Akka.Cluster.Cluster.Get(system).SelfAddress;
            var prefix = $"{origin.Host}-{origin.Port}";
            var sequence = 0;

            system.Scheduler.Advanced.ScheduleRepeatedly(StartupDelay, SubmitInterval, () =>
            {
                var id = new JobId($"{prefix}-{Interlocked.Increment(ref sequence)}");
                var size = new JobSize((uint)Random.Shared.Next((int)MinJobSize, (int)MaxJobSize));
                var submitter = new JobSubmitterId(
                    $"submitter-{Random.Shared.Next(1, SubmitterCount + 1)}");

                submitters.Tell(new JobTrackerCommands.SubmitJob(
                    new JobDefinition(id, size),
                    submitter));
            });

            return Task.CompletedTask;
        });
}
