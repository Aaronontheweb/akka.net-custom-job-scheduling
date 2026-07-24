using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// The same registration extension methods, on real cluster infrastructure.
/// </summary>
/// <remarks>
/// <para>
/// The rest of the suite runs in <see cref="AkkaExecutionMode.LocalTest"/>, which is fast but
/// proves nothing about the clustered branch of each <c>With*</c> method. This class is the
/// counterweight: one node, self-joined, with a real cluster singleton, a real shard region, and
/// the real <see cref="Cluster.ClusterEventTranslator"/> turning
/// <c>ClusterEvent.MemberUp</c> into <see cref="JobTrackerCommands.NodeJoined"/>.
/// </para>
/// <para>
/// It is deliberately thin — enough to catch a registration branch that doesn't compose, not a
/// second copy of the behavioural suite.
/// </para>
/// </remarks>
[Trait("ExecutionMode", "Clustered")]
public class ClusteredSchedulingTests : JobSchedulingTestKit
{
    public ClusteredSchedulingTests(ITestOutputHelper output) : base(output)
    {
    }

    protected override AkkaExecutionMode ExecutionMode => AkkaExecutionMode.Clustered;

    [Fact]
    public async Task The_tracker_singleton_learns_this_node_from_real_cluster_events()
    {
        await JoinClusterAsync();

        // Nothing told the tracker about this node — the ClusterEventTranslator did, off a real
        // MemberUp, using the capacity supplied at registration.
        await AwaitAssertAsync(async () =>
        {
            var queue = await QueueAsync();
            Assert.Single(queue.Nodes);
            Assert.Equal(
                new JobSize(JobSchedulingHostingExtensions.DefaultNodeCapacity),
                queue.TotalCapacity);
        });
    }

    [Fact]
    public async Task A_job_submitted_through_the_shard_region_runs_to_completion()
    {
        await JoinClusterAsync();
        Pacer.RunToCompletion();

        Submitters.Tell(new JobTrackerCommands.SubmitJob(
            Job("clustered-job-1", 30),
            new JobSubmitterId("submitter-1")));

        var status = await AwaitStatusAsync("clustered-job-1", JobStatus.Completed);
        Assert.Equal(new JobSubmitterId("submitter-1"), status.SubmitterId);
        Assert.Equal(1m, status.Progress.Progress.Fraction);
    }
}
