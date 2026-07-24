using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.TestKit;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// Tracker → receiver → executor → back to tracker, with topology driven by hand.
/// </summary>
/// <remarks>
/// Every one of these would ordinarily need a multi-node cluster: two worker nodes, a job placed on
/// the roomier one, a node downed mid-flight, work migrating to the survivor. All of it runs in one
/// process in milliseconds because the only cluster-shaped dependency is behind
/// <c>IClusterMembershipSource</c>.
/// </remarks>
public class EndToEndSchedulingTests : JobSchedulingTestKit
{
    public EndToEndSchedulingTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task A_submitted_job_is_dispatched_to_a_node_and_starts_running()
    {
        Membership.MemberUp("node-a", 100);

        await SubmitAsync("job-1", 40);

        var status = await AwaitStatusAsync("job-1", JobStatus.Running);
        Assert.Equal(NodeAddress("node-a"), status.AssignedNode);
    }

    [Fact]
    public async Task Placement_prefers_the_node_with_the_most_available_capacity()
    {
        Membership.MemberUp("node-a", 100);
        Membership.MemberUp("node-b", 100);

        // Ties break ordinally, so job-1 lands on node-a and leaves it with 40 free.
        await SubmitAsync("job-1", 60);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        await SubmitAsync("job-2", 30);

        var status = await AwaitStatusAsync("job-2", JobStatus.Running);
        Assert.Equal(NodeAddress("node-b"), status.AssignedNode);
    }

    [Fact]
    public async Task A_job_runs_to_completion_and_frees_its_capacity()
    {
        Membership.MemberUp("node-a", 100);
        Pacer.RunToCompletion();

        await SubmitAsync("job-1", 40);

        var status = await AwaitStatusAsync("job-1", JobStatus.Completed);
        Assert.Equal(1m, status.Progress.Progress.Fraction);
        Assert.Null(status.AssignedNode);

        await AwaitAssertAsync(async () =>
        {
            var queue = await QueueAsync();
            Assert.Equal(0, queue.RunningCount);
            Assert.Equal(new JobSize(100), queue.AvailableCapacity);
        });
    }

    [Fact]
    public async Task Executors_report_intermediate_progress()
    {
        Membership.MemberUp("node-a", 100);
        Pacer.RunToCompletion(unitsPerTick: 1);

        var probe = CreateTestProbe();
        await SubmitAsync("job-1", 5);
        Tracker.Tell(new JobTrackerQueries.SubscribeToJob(new JobId("job-1"), probe));

        // Somewhere between "just started" and "finished" the tracker must see partial progress.
        await probe.FishForMessageAsync<JobTrackerNotifications.JobStatusChanged>(
            notification => notification.Progress.Status == JobStatus.Running
                            && notification.Progress.Progress.Completed > JobSize.Zero,
            RemainingOrDefault);
    }

    [Fact]
    public async Task Queued_work_starts_as_soon_as_a_running_job_completes()
    {
        Membership.MemberUp("node-a", 100);

        await SubmitAsync("job-1", 80);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        // Doesn't fit alongside job-1.
        await SubmitAsync("job-2", 50);
        Assert.Equal(JobStatus.Waiting, (await FoundAsync("job-2")).Progress.Status);

        Tracker.Tell(new JobTrackerCommands.CancelJob(
            new JobId("job-1"),
            new JobSubmitterId("submitter-1"),
            "make room"));

        var status = await AwaitStatusAsync("job-2", JobStatus.Running);
        Assert.Equal(NodeAddress("node-a"), status.AssignedNode);
    }

    [Fact]
    public async Task Losing_a_node_mid_flight_moves_its_work_to_the_survivor()
    {
        Membership.MemberUp("node-a", 100);
        Membership.MemberUp("node-b", 100);

        await SubmitAsync("job-1", 60);
        var placed = await AwaitStatusAsync("job-1", JobStatus.Running);
        Assert.Equal(NodeAddress("node-a"), placed.AssignedNode);

        Membership.MemberRemoved("node-a");

        await AwaitAssertAsync(async () =>
        {
            var moved = await FoundAsync("job-1");
            Assert.Equal(JobStatus.Running, moved.Progress.Status);
            Assert.Equal(NodeAddress("node-b"), moved.AssignedNode);
        });
    }

    [Fact]
    public async Task An_unreachable_node_keeps_its_work_until_it_is_removed()
    {
        Membership.MemberUp("node-a", 100);
        Membership.MemberUp("node-b", 100);

        await SubmitAsync("job-1", 60);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        Membership.Unreachable("node-a");

        // Still on node-a: the partition may heal, and node-a may still be working on it.
        await Task.Delay(200);
        var held = await FoundAsync("job-1");
        Assert.Equal(NodeAddress("node-a"), held.AssignedNode);

        // Now the cluster gives up on it.
        Membership.MemberRemoved("node-a");

        await AwaitAssertAsync(async () =>
        {
            var moved = await FoundAsync("job-1");
            Assert.Equal(NodeAddress("node-b"), moved.AssignedNode);
        });
    }

    [Fact]
    public async Task The_submitter_entity_receives_progress_for_the_job_it_asked_for()
    {
        Membership.MemberUp("node-a", 100);
        Pacer.RunToCompletion();

        // Route through the submitter region rather than talking to the tracker directly — this is
        // the path a real request would take, and it exercises the message extractor.
        Submitters.Tell(new JobTrackerCommands.SubmitJob(
            Job("job-1", 40),
            new JobSubmitterId("submitter-7")));

        var status = await AwaitStatusAsync("job-1", JobStatus.Completed);
        Assert.Equal(new JobSubmitterId("submitter-7"), status.SubmitterId);
    }
}
