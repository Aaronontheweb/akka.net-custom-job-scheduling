using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.TestKit;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// The tracker actor as a shell over <see cref="JobTrackerState"/>: does it wire the clock, the
/// membership source, the receivers, and the subscribers together correctly?
/// </summary>
/// <remarks>
/// The scheduling <i>rules</i> are already proven in <c>Core.Tests</c> without an ActorSystem.
/// These tests deliberately don't re-litigate them — they cover only what the actor adds.
/// </remarks>
public class JobTrackerActorTests : JobSchedulingTestKit
{
    public JobTrackerActorTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Tracker_learns_about_nodes_from_the_membership_source()
    {
        Membership.MemberUp("node-a", 100);
        Membership.MemberUp("node-b", 60);

        await AwaitAssertAsync(async () =>
        {
            var queue = await QueueAsync();
            Assert.Equal(new JobSize(160), queue.TotalCapacity);
            Assert.Equal(2, queue.Nodes.Length);
        });
    }

    [Fact]
    public async Task Membership_that_happened_before_the_tracker_subscribed_is_still_seen()
    {
        // The tracker subscribes in PreStart, which has already run by the time a test body
        // executes — so this is really asserting the history replay in LocalClusterMembershipSource,
        // which stands in for the CurrentClusterState snapshot a real subscriber receives.
        Membership.MemberUp("node-a", 100);
        Membership.MemberRemoved("node-a");
        Membership.MemberUp("node-b", 75);

        await AwaitAssertAsync(async () =>
        {
            var queue = await QueueAsync();
            Assert.Single(queue.Nodes);
            Assert.Equal(new JobSize(75), queue.TotalCapacity);
        });
    }

    [Fact]
    public async Task Submitting_with_no_nodes_is_accepted_and_queued()
    {
        var response = await SubmitAsync("job-1", 40);

        Assert.IsType<JobTrackerResponses.CommandAccepted>(response);

        var status = await FoundAsync("job-1");
        Assert.Equal(JobStatus.Waiting, status.Progress.Status);
        Assert.Null(status.AssignedNode);
    }

    [Fact]
    public async Task Submitting_a_duplicate_is_rejected()
    {
        await SubmitAsync("job-1", 40);

        var response = await SubmitAsync("job-1", 40);

        var rejected = Assert.IsType<JobTrackerResponses.CommandRejected>(response);
        Assert.Equal(JobRejectionReason.DuplicateJobId, rejected.Reason);
    }

    [Fact]
    public async Task Unknown_jobs_report_not_found()
    {
        Assert.IsType<JobTrackerQueryResponses.JobNotFound>(await StatusAsync("nope"));
    }

    [Fact]
    public async Task A_subscriber_receives_the_current_status_immediately_on_subscribing()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 40);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        var probe = CreateTestProbe();
        Tracker.Tell(new JobTrackerQueries.SubscribeToJob(new JobId("job-1"), probe));

        var notification = await probe.ExpectMsgAsync<JobTrackerNotifications.JobStatusChanged>();
        Assert.Equal(new JobId("job-1"), notification.Id);
        Assert.Equal(JobStatus.Running, notification.Progress.Status);
    }

    [Fact]
    public async Task A_subscriber_is_pushed_every_subsequent_transition()
    {
        Membership.MemberUp("node-a", 100);
        Pacer.RunToCompletion();

        var probe = CreateTestProbe();
        await SubmitAsync("job-1", 40);
        Tracker.Tell(new JobTrackerQueries.SubscribeToJob(new JobId("job-1"), probe));

        await probe.FishForMessageAsync<JobTrackerNotifications.JobStatusChanged>(
            notification => notification.Progress.Status == JobStatus.Completed,
            RemainingOrDefault);
    }

    [Fact]
    public async Task Unsubscribing_stops_the_pushes()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 40);

        var probe = CreateTestProbe();
        Tracker.Tell(new JobTrackerQueries.SubscribeToJob(new JobId("job-1"), probe));
        await probe.ExpectMsgAsync<JobTrackerNotifications.JobStatusChanged>();

        var ack = await Tracker.Ask<JobTrackerQueryResponses.UnsubscribeAck>(
            new JobTrackerQueries.UnsubscribeFromJob(new JobId("job-1"), probe),
            RemainingOrDefault);
        Assert.Equal(new JobId("job-1"), ack.Id);

        Tracker.Tell(new JobTrackerCommands.CancelJob(
            new JobId("job-1"),
            new JobSubmitterId("submitter-1"),
            "done testing"));

        await probe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300));
    }

    [Fact]
    public async Task Queue_status_reflects_placement()
    {
        Membership.MemberUp("node-a", 100);

        await SubmitAsync("job-1", 60);
        await AwaitStatusAsync("job-1", JobStatus.Running);
        await SubmitAsync("job-2", 80);
        await AwaitStatusAsync("job-2", JobStatus.Queued);

        var queue = await QueueAsync();

        Assert.Equal(1, queue.RunningCount);
        Assert.Equal(0, queue.WaitingCount);   // job-2 is committed to node-a's queue, not global
        Assert.Equal(1, queue.QueuedCount);
        Assert.Equal(new JobSize(80), queue.QueuedWork);
        Assert.Equal(new JobSize(40), queue.AvailableCapacity);
    }

    [Fact]
    public async Task Adding_a_worker_runs_queued_work_without_moving_running_work()
    {
        Membership.MemberUp("node-a", 100);
        await AwaitAssertAsync(async () =>
            Assert.Single((await QueueAsync()).Nodes));

        await SubmitAsync("running", 100);
        var running = await AwaitStatusAsync("running", JobStatus.Running);
        Assert.Equal(NodeAddress("node-a"), running.AssignedNode);

        await SubmitAsync("queued", 100);
        var queued = await AwaitStatusAsync("queued", JobStatus.Queued);
        Assert.Equal(NodeAddress("node-a"), queued.AssignedNode);

        Membership.MemberUp("node-b", 100);

        queued = await AwaitStatusAsync("queued", JobStatus.Running);
        running = await FoundAsync("running");
        Assert.Equal(NodeAddress("node-b"), queued.AssignedNode);
        Assert.Equal(JobStatus.Running, running.Progress.Status);
        Assert.Equal(NodeAddress("node-a"), running.AssignedNode);
    }
}
