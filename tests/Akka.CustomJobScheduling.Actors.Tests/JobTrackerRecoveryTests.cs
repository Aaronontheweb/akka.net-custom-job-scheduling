using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// What survives a tracker restart, and what gets corrected on the way back up.
/// </summary>
/// <remarks>
/// The tracker is a cluster singleton, so "restart" here stands in for a failover onto another
/// host: same persistence id, same journal, a process that has never seen any of this state
/// before. In-memory journals in Akka.Hosting.TestKit are per-ActorSystem rather than per-actor, so
/// stopping and recreating the actor replays exactly what a new host would read.
/// </remarks>
public class JobTrackerRecoveryTests : JobSchedulingTestKit
{
    public JobTrackerRecoveryTests(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Kills the tracker and stands up a replacement on the same persistence id.
    /// </summary>
    private async Task<IActorRef> RestartTrackerAsync()
    {
        var original = Tracker;
        await WatchAsync(original);
        original.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(original);

        var replacement = Sys.ActorOf(
            Props.Create(() => new JobTrackerActor(
                Membership,
                new LocalJobReceiverRouterProbe(Sys),
                new RequiredActorStub(ActorRegistry.Get<JobSubmitterManagerKey>()),
                TimeProvider.System)),
            $"job-tracker-{Guid.NewGuid():N}");

        return replacement;
    }

    [Fact]
    public async Task State_survives_a_restart_after_the_journal_is_snapshotted_and_trimmed()
    {
        // Drive more than SnapshotEvery (100) events through the tracker so it snapshots and then
        // trims the journal underneath itself. Each running submission persists three events
        // (JobAccepted + JobQueued + JobScheduled), so ~40 jobs comfortably crosses the boundary.
        Membership.MemberUp("node-a", 1000);

        const int jobs = 40;
        for (var i = 0; i < jobs; i++)
            await SubmitAsync($"job-{i:D2}", 1);

        await AwaitStatusAsync($"job-{jobs - 1:D2}", JobStatus.Running);

        // Failover. Recovery has to load the snapshot and replay only what followed it; if the
        // DeleteMessages trim removed anything the snapshot didn't already cover, a job would come
        // back missing or in the wrong state.
        var restarted = await RestartTrackerAsync();

        foreach (var id in new[] { "job-00", "job-20", $"job-{jobs - 1:D2}" })
        {
            var status = await restarted.Ask<IJobTrackerQueryResponse>(
                new JobTrackerQueries.GetJobStatus(new JobId(id)),
                RemainingOrDefault);

            var found = Assert.IsType<JobTrackerQueryResponses.JobStatusResult>(status);
            Assert.Equal(JobStatus.Running, found.Progress.Status);
            Assert.Equal(NodeAddress("node-a"), found.AssignedNode);
        }
    }

    [Fact]
    public async Task Accepted_jobs_survive_a_restart()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("survivor", 40);
        await AwaitStatusAsync("survivor", JobStatus.Running);

        var restarted = await RestartTrackerAsync();

        var status = await restarted.Ask<IJobTrackerQueryResponse>(
            new JobTrackerQueries.GetJobStatus(new JobId("survivor")),
            RemainingOrDefault);

        var found = Assert.IsType<JobTrackerQueryResponses.JobStatusResult>(status);
        Assert.Equal(JobStatus.Running, found.Progress.Status);
        Assert.Equal(NodeAddress("node-a"), found.AssignedNode);
    }

    [Fact]
    public async Task Progress_reported_before_a_restart_is_not_lost()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("tracked", 10);
        await AwaitStatusAsync("tracked", JobStatus.Running);

        Tracker.Tell(new JobTrackerFacts.ProgressReported(
            new JobId("tracked"),
            NodeAddress("node-a"),
            new WorkProgress(new JobSize(6), new JobSize(10))),
            ActorRefs.NoSender);

        await AwaitAssertAsync(async () =>
            Assert.Equal(0.6m, (await FoundAsync("tracked")).Progress.Progress.Fraction));

        var restarted = await RestartTrackerAsync();

        var status = (JobTrackerQueryResponses.JobStatusResult)
            await restarted.Ask<IJobTrackerQueryResponse>(
                new JobTrackerQueries.GetJobStatus(new JobId("tracked")),
                RemainingOrDefault);

        Assert.Equal(0.6m, status.Progress.Progress.Fraction);
    }

    [Fact]
    public async Task Capacity_accounting_is_rebuilt_from_the_journal()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 60);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        var restarted = await RestartTrackerAsync();

        var queue = await restarted.Ask<JobTrackerQueryResponses.QueueStatus>(
            JobTrackerQueries.GetQueueStatus.Instance,
            RemainingOrDefault);

        // Not just the node and the job: the 60 units the job committed have to come back too, or
        // the replacement tracker would cheerfully oversubscribe the node.
        Assert.Single(queue.Nodes);
        Assert.Equal(new JobSize(60), queue.Nodes[0].CapacityInUse);
        Assert.Equal(new JobSize(40), queue.AvailableCapacity);
    }

    [Fact]
    public async Task A_node_that_left_while_the_tracker_was_down_is_reconciled_away()
    {
        Membership.MemberUp("node-a", 100);
        Membership.MemberUp("node-b", 100);
        await SubmitAsync("stranded", 60);
        var placed = await AwaitStatusAsync("stranded", JobStatus.Running);
        Assert.Equal(NodeAddress("node-a"), placed.AssignedNode);

        // node-a disappears while nothing is listening. No NodeLeft is ever delivered, so replay
        // alone would restore a node that no longer exists and leave the job pinned to it.
        Membership.Forget("node-a");

        var restarted = await RestartTrackerAsync();

        // On subscribe the membership source reports the surviving set, and the tracker reconciles:
        // node-a is dropped and its work is requeued onto node-b.
        await AwaitAssertAsync(async () =>
        {
            var status = (JobTrackerQueryResponses.JobStatusResult)
                await restarted.Ask<IJobTrackerQueryResponse>(
                    new JobTrackerQueries.GetJobStatus(new JobId("stranded")),
                    RemainingOrDefault);

            Assert.Equal(NodeAddress("node-b"), status.AssignedNode);
        });
    }
}

/// <summary>Router that resolves nothing; recovery tests care about state, not dispatch.</summary>
internal sealed class LocalJobReceiverRouterProbe : Core.Actors.Cluster.IJobReceiverRouter
{
    private readonly ActorSystem _system;

    public LocalJobReceiverRouterProbe(ActorSystem system) => _system = system;

    public ActorSelection Select(Address nodeAddress) =>
        _system.ActorSelection(Core.Actors.Cluster.LocalJobReceiverRouter.PathFor(nodeAddress));
}

/// <summary>Hands a known ref to the tracker without going through Akka.Hosting's resolver.</summary>
internal sealed class RequiredActorStub : Akka.Hosting.IRequiredActor<JobSubmitterManagerKey>
{
    public RequiredActorStub(IActorRef actorRef) => ActorRef = actorRef;

    public IActorRef ActorRef { get; }

    public Task<IActorRef> GetAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(ActorRef);
}
