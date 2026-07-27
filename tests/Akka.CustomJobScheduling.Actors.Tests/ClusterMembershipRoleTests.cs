using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Hosting;
using Akka.TestKit;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/*
 * Regression tests for a real bug.
 *
 * The translator originally reported every cluster member as capacity. Once an API tier joined the
 * same cluster under the "api" role - hosting no JobReceiverActor - the tracker counted it as 100
 * units, placed a job there, and the job sat in Running at zero progress forever because nothing on
 * that node was listening.
 *
 * These drive ClusterMembershipSource against a real single-node cluster and assert on what it
 * publishes. They deliberately avoid the tracker: the singleton is pinned to the worker role, so on
 * an api-only node there is nothing to ask.
 */

/// <summary>
/// A node carrying the worker role is reported as capacity.
/// </summary>
[Trait("ExecutionMode", "Clustered")]
public class WorkerRoleMembershipTests : Akka.Hosting.TestKit.TestKit
{
    public WorkerRoleMembershipTests(ITestOutputHelper output) : base(output: output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.AddHocon(ClusterRoleHocon.For("worker"), HoconAddMode.Prepend);

    [Fact]
    public async Task Includes_a_worker_in_the_membership_snapshot()
    {
        // Join before subscribing. The translator derives SyncNodes from the CurrentClusterState
        // snapshot it receives at subscribe time, so subscribing first is a race: if the snapshot
        // is taken before the node reaches Up, membership is legitimately empty and the assertion
        // has nothing to look at.
        await ClusterRoleHocon.JoinAsync(this);

        var probe = CreateTestProbe();
        new ClusterMembershipSource(Sys, new JobSize(100), "worker").Subscribe(probe);

        var sync = await probe.ExpectMsgAsync<JobTrackerCommands.SyncNodes>(
            TimeSpan.FromSeconds(10));

        Assert.Equal(new JobSize(100), sync.Members[Akka.Cluster.Cluster.Get(Sys).SelfAddress]);
    }
}

/// <summary>
/// A node that joins for some other reason is not.
/// </summary>
[Trait("ExecutionMode", "Clustered")]
public class NonWorkerRoleMembershipTests : Akka.Hosting.TestKit.TestKit
{
    public NonWorkerRoleMembershipTests(ITestOutputHelper output) : base(output: output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.AddHocon(ClusterRoleHocon.For("api"), HoconAddMode.Prepend);

    [Fact]
    public async Task Leaves_a_non_worker_out_of_the_membership_snapshot()
    {
        // Join first for the same reason as the worker case, and here it also makes the test mean
        // something: an empty result only proves the role filter works if the node was definitely
        // Up when the snapshot was taken.
        await ClusterRoleHocon.JoinAsync(this);

        var probe = CreateTestProbe();
        new ClusterMembershipSource(Sys, new JobSize(100), "worker").Subscribe(probe);

        // The node is Up and the translator saw it — it just isn't ours to schedule onto, so the
        // reconciliation reports an empty worker set rather than omitting the message.
        var sync = await probe.ExpectMsgAsync<JobTrackerCommands.SyncNodes>(
            TimeSpan.FromSeconds(10));

        Assert.Empty(sync.Members);
        await probe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(500));
    }
}

internal static class ClusterRoleHocon
{
    public static string For(string role) =>
        $$"""
          akka.actor.provider = cluster
          akka.remote.dot-netty.tcp.hostname = "127.0.0.1"
          akka.remote.dot-netty.tcp.port = 0
          akka.cluster.seed-nodes = []
          akka.cluster.roles = ["{{role}}"]
          """;

    public static async Task JoinAsync(Akka.Hosting.TestKit.TestKit kit)
    {
        var cluster = Akka.Cluster.Cluster.Get(kit.Sys);
        cluster.Join(cluster.SelfAddress);

        await kit.AwaitConditionAsync(
            () => Task.FromResult(cluster.SelfMember.Status == MemberStatus.Up),
            TimeSpan.FromSeconds(15));
    }
}
