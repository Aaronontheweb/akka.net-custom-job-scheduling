using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// Base fixture for job scheduling actor tests.
/// </summary>
/// <remarks>
/// Runs the <i>same</i> registration extension methods production uses, only with
/// <see cref="AkkaExecutionMode.LocalTest"/>. The two substitutions that make a cluster
/// unnecessary — <see cref="LocalClusterMembershipSource"/> and
/// <see cref="TestJobExecutionPacer"/> — go in through DI, so no actor knows it's under test.
/// </remarks>
public abstract class JobSchedulingTestKit : Akka.Hosting.TestKit.TestKit
{
    protected JobSchedulingTestKit(ITestOutputHelper output) : base(output: output)
    {
    }

    protected virtual AkkaExecutionMode ExecutionMode => AkkaExecutionMode.LocalTest;

    /// <summary>Fake worker nodes stood up inside this single ActorSystem.</summary>
    protected virtual IReadOnlyList<string> Nodes => ["node-a", "node-b"];

    /// <summary>Drives cluster topology by hand. This is the whole point of the fixture.</summary>
    protected LocalClusterMembershipSource Membership { get; } = new();

    protected TestJobExecutionPacer Pacer { get; } = new();

    protected IActorRef Tracker => ActorRegistry.Get<JobTrackerKey>();

    protected IActorRef Submitters => ActorRegistry.Get<JobSubmitterManagerKey>();

    protected static Address NodeAddress(string host) =>
        LocalClusterMembershipSource.NodeAddress(host);

    /// <summary>
    /// HOCON for a single-node self-joining cluster, used only by
    /// <see cref="AkkaExecutionMode.Clustered"/> subclasses.
    /// </summary>
    private const string SingleNodeClusterHocon =
        """
        akka.actor.provider = cluster
        akka.remote.dot-netty.tcp.hostname = "127.0.0.1"
        akka.remote.dot-netty.tcp.port = 0
        akka.cluster.seed-nodes = []
        akka.cluster.sharding.state-store-mode = ddata
        akka.cluster.sharding.remember-entities = off
        """;

    /// <summary>
    /// Cluster roles this node carries. Override to model a node that joins the cluster without
    /// running work.
    /// </summary>
    protected virtual IReadOnlyList<string> ClusterRoles => [JobSchedulingHostingExtensions.WorkerRole];

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        // Registered before AddJobSchedulingServices, which only fills in what's missing. In
        // clustered mode we deliberately let the real ClusterMembershipSource win, so the
        // ClusterEventTranslator gets exercised against actual Akka.Cluster events.
        if (ExecutionMode == AkkaExecutionMode.LocalTest)
            services.AddSingleton<IClusterMembershipSource>(Membership);

        services.AddSingleton<IJobExecutionPacer>(Pacer);
        services.AddJobSchedulingServices(ExecutionMode);
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (ExecutionMode == AkkaExecutionMode.Clustered)
        {
            var roles = string.Join(", ", ClusterRoles.Select(role => $"\"{role}\""));
            builder.AddHocon(
                $"{SingleNodeClusterHocon}\nakka.cluster.roles = [{roles}]",
                HoconAddMode.Prepend);
        }

        builder.WithJobSchedulingActors(ExecutionMode, Nodes);
    }

    /// <summary>
    /// Forms a one-node cluster and waits for it to reach <c>Up</c>. Clustered tests must call this
    /// before touching the tracker — the singleton doesn't start until the node is a member.
    /// </summary>
    protected async Task JoinClusterAsync()
    {
        var cluster = Akka.Cluster.Cluster.Get(Sys);
        cluster.Join(cluster.SelfAddress);

        await AwaitConditionAsync(
            () => Task.FromResult(cluster.SelfMember.Status == MemberStatus.Up),
            TimeSpan.FromSeconds(15));
    }

    // ---- helpers ----

    protected static JobDefinition Job(string id, uint size) =>
        new(new JobId(id), new JobSize(size));

    protected Task<IJobTrackerCommandResponse> SubmitAsync(
        string id,
        uint size,
        string submitter = "submitter-1") =>
        Tracker.Ask<IJobTrackerCommandResponse>(
            new JobTrackerCommands.SubmitJob(Job(id, size), new JobSubmitterId(submitter)),
            RemainingOrDefault);

    protected Task<IJobTrackerQueryResponse> StatusAsync(string id) =>
        Tracker.Ask<IJobTrackerQueryResponse>(
            new JobTrackerQueries.GetJobStatus(new JobId(id)),
            RemainingOrDefault);

    protected async Task<JobTrackerQueryResponses.JobStatusResult> FoundAsync(string id) =>
        Assert.IsType<JobTrackerQueryResponses.JobStatusResult>(await StatusAsync(id));

    protected Task<JobTrackerQueryResponses.QueueStatus> QueueAsync() =>
        Tracker.Ask<JobTrackerQueryResponses.QueueStatus>(
            JobTrackerQueries.GetQueueStatus.Instance,
            RemainingOrDefault);

    /// <summary>
    /// Waits until <paramref name="id"/> reaches <paramref name="status"/>, then returns it.
    /// </summary>
    protected async Task<JobTrackerQueryResponses.JobStatusResult> AwaitStatusAsync(
        string id,
        JobStatus status)
    {
        JobTrackerQueryResponses.JobStatusResult? result = null;

        await AwaitAssertAsync(async () =>
        {
            result = await FoundAsync(id);
            Assert.Equal(status, result.Progress.Status);
        });

        return result!;
    }
}
