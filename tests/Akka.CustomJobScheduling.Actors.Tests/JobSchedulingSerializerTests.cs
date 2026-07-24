using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.CustomJobScheduling.Core.Serialization;
using Akka.Hosting;
using Akka.Serialization;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// Round-trips every type the serializer claims to handle.
/// </summary>
/// <remarks>
/// Worth more than it looks. Anything that survives a round-trip here is safe in the journal
/// forever; anything that doesn't is a recovery failure discovered at 3am instead. The manifest
/// assertions matter just as much as the payloads — those strings are permanent, and a rename
/// orphans every event already written under the old one.
/// </remarks>
public class JobSchedulingSerializerTests : Akka.Hosting.TestKit.TestKit
{
    private static readonly DateTimeOffset At = new(2026, 7, 24, 12, 0, 0, TimeSpan.Zero);
    private static readonly Address NodeA = new("akka", "JobScheduling", "node-a", 2552);
    private static readonly Address NodeB = new("akka", "JobScheduling", "node-b", 2552);

    public JobSchedulingSerializerTests(ITestOutputHelper output) : base(output: output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.AddJobSchedulingSerializer();

    private static JobDefinition Job(string id, uint size) => new(new JobId(id), new JobSize(size));

    private static WorkProgress Progress(uint done, uint total) =>
        new(new JobSize(done), new JobSize(total));

    private static JobProgress JobProgress(string id, JobStatus status) =>
        new(new JobId(id), status, Progress(3, 10), At);

    public static TheoryData<object, string> Payloads() => new()
    {
        // events - the set that must survive forever
        { new JobTrackerEvents.NodeAdded(NodeA, MemberStatus.Up, new JobSize(100), At), JobSchedulingManifests.NodeAdded },
        { new JobTrackerEvents.NodeRemoved(NodeA, At), JobSchedulingManifests.NodeRemoved },
        { new JobTrackerEvents.NodeStatusChanged(NodeA, MemberStatus.WeaklyUp, false, At), JobSchedulingManifests.NodeStatusChanged },
        { new JobTrackerEvents.JobAccepted(Job("j1", 40), new JobSubmitterId("s1"), At), JobSchedulingManifests.JobAccepted },
        { new JobTrackerEvents.JobScheduled(new JobId("j1"), NodeA, At), JobSchedulingManifests.JobScheduled },
        { new JobTrackerEvents.JobProgressed(new JobId("j1"), Progress(5, 40), At), JobSchedulingManifests.JobProgressed },
        { new JobTrackerEvents.JobCompleted(new JobId("j1"), At), JobSchedulingManifests.JobCompleted },
        { new JobTrackerEvents.JobFailed(new JobId("j1"), "boom", At), JobSchedulingManifests.JobFailed },
        { new JobTrackerEvents.JobCancelled(new JobId("j1"), "changed my mind", At), JobSchedulingManifests.JobCancelled },
        { new JobTrackerEvents.JobRequeued(new JobId("j1"), NodeA, "node left", At), JobSchedulingManifests.JobRequeued },

        // commands
        { new JobTrackerCommands.SubmitJob(Job("j1", 40), new JobSubmitterId("s1")), JobSchedulingManifests.SubmitJob },
        { new JobTrackerCommands.CancelJob(new JobId("j1"), new JobSubmitterId("s1"), "why"), JobSchedulingManifests.CancelJob },
        { new JobTrackerCommands.ReportProgress(new JobId("j1"), NodeA, Progress(1, 4)), JobSchedulingManifests.ReportProgress },
        { new JobTrackerCommands.ReportJobCompleted(new JobId("j1"), NodeA), JobSchedulingManifests.ReportJobCompleted },
        { new JobTrackerCommands.ReportJobFailed(new JobId("j1"), NodeA, "nope"), JobSchedulingManifests.ReportJobFailed },
        { new JobTrackerCommands.NodeJoined(NodeA, MemberStatus.Up, new JobSize(75)), JobSchedulingManifests.NodeJoined },
        { new JobTrackerCommands.NodeLeft(NodeA), JobSchedulingManifests.NodeLeft },
        { new JobTrackerCommands.NodeReachabilityChanged(NodeA, true), JobSchedulingManifests.NodeReachabilityChanged },
        { JobTrackerCommands.DrainQueue.Instance, JobSchedulingManifests.DrainQueue },

        // command responses
        { new JobTrackerResponses.CommandAccepted(new JobId("j1")), JobSchedulingManifests.CommandAccepted },
        { new JobTrackerResponses.CommandRejected(new JobId("j1"), JobRejectionReason.DuplicateJobId, "dupe"), JobSchedulingManifests.CommandRejected },

        // queries and replies without actor refs
        { new JobTrackerQueries.GetJobStatus(new JobId("j1")), JobSchedulingManifests.GetJobStatus },
        { JobTrackerQueries.GetQueueStatus.Instance, JobSchedulingManifests.GetQueueStatus },
        { new JobTrackerQueryResponses.JobNotFound(new JobId("j1")), JobSchedulingManifests.JobNotFound },
        { new JobTrackerQueryResponses.JobStatusResult(new JobId("j1"), new JobSubmitterId("s1"), JobProgress("j1", JobStatus.Running), NodeA), JobSchedulingManifests.JobStatusResult },

        // notifications
        { new JobTrackerNotifications.JobStatusChanged(new JobId("j1"), new JobSubmitterId("s1"), JobProgress("j1", JobStatus.Completed), null), JobSchedulingManifests.JobStatusChanged },

        // execution protocol
        { new ExecutionMessages.ExecuteJob(Job("j1", 40)), JobSchedulingManifests.ExecuteJob },
        { new ExecutionMessages.CancelExecution(new JobId("j1"), "stop"), JobSchedulingManifests.CancelExecution }
    };

    [Theory]
    [MemberData(nameof(Payloads))]
    public void Round_trips_with_a_stable_manifest(object payload, string expectedManifest)
    {
        var serializer = Assert.IsType<JobSchedulingSerializer>(
            Sys.Serialization.FindSerializerFor(payload));

        var manifest = serializer.Manifest(payload);
        Assert.Equal(expectedManifest, manifest);

        var bytes = serializer.ToBinary(payload);
        var restored = serializer.FromBinary(bytes, manifest);

        Assert.Equal(payload, restored);
    }

    [Fact]
    public void Round_trips_a_node_sync()
    {
        var command = new JobTrackerCommands.SyncNodes(ImmutableDictionary<Address, JobSize>.Empty
            .Add(NodeA, new JobSize(100))
            .Add(NodeB, new JobSize(60)));

        var restored = (JobTrackerCommands.SyncNodes)RoundTrip(command);

        Assert.Equal(new JobSize(100), restored.Members[NodeA]);
        Assert.Equal(new JobSize(60), restored.Members[NodeB]);
    }

    [Fact]
    public void Round_trips_a_queue_status_with_nodes()
    {
        var status = new JobTrackerQueryResponses.QueueStatus(
            WaitingCount: 2,
            RunningCount: 1,
            QueuedWork: new JobSize(30),
            TotalCapacity: new JobSize(200),
            AvailableCapacity: new JobSize(140),
            Nodes:
            [
                new NodeStatus(NodeA, MemberStatus.Up, true, At, new JobSize(100), new JobSize(60)),
                new NodeStatus(NodeB, MemberStatus.WeaklyUp, false, At, new JobSize(100), JobSize.Zero)
            ]);

        var restored = (JobTrackerQueryResponses.QueueStatus)RoundTrip(status);

        Assert.Equal(2, restored.WaitingCount);
        Assert.Equal(new JobSize(140), restored.AvailableCapacity);
        // ImmutableArray<T> equality is reference-based on the backing array, so compare elementwise.
        Assert.Equal<NodeStatus>(status.Nodes.AsEnumerable(), restored.Nodes.AsEnumerable());
    }

    [Fact]
    public void Round_trips_actor_references_in_subscriptions()
    {
        var probe = CreateTestProbe();

        var subscribe = (JobTrackerQueries.SubscribeToJob)
            RoundTrip(new JobTrackerQueries.SubscribeToJob(new JobId("j1"), probe.Ref));
        Assert.Equal(probe.Ref, subscribe.Subscriber);

        var unsubscribe = (JobTrackerQueries.UnsubscribeFromJob)
            RoundTrip(new JobTrackerQueries.UnsubscribeFromJob(new JobId("j1"), probe.Ref));
        Assert.Equal(probe.Ref, unsubscribe.Subscriber);

        var ack = (JobTrackerQueryResponses.SubscribeAck)
            RoundTrip(new JobTrackerQueryResponses.SubscribeAck(new JobId("j1"), probe.Ref));
        Assert.Equal(probe.Ref, ack.Subscriber);
    }

    [Fact]
    public void Round_trips_tracker_state_for_snapshots()
    {
        var state = JobTrackerState.Empty
            .Fold(
            [
                new JobTrackerEvents.NodeAdded(NodeA, MemberStatus.Up, new JobSize(100), At),
                new JobTrackerEvents.NodeAdded(NodeB, MemberStatus.Up, new JobSize(50), At),
                new JobTrackerEvents.JobAccepted(Job("running", 60), new JobSubmitterId("s1"), At),
                new JobTrackerEvents.JobScheduled(new JobId("running"), NodeA, At),
                new JobTrackerEvents.JobProgressed(new JobId("running"), Progress(20, 60), At),
                new JobTrackerEvents.JobAccepted(Job("queued", 90), new JobSubmitterId("s2"), At),
                new JobTrackerEvents.JobAccepted(Job("done", 5), new JobSubmitterId("s1"), At),
                new JobTrackerEvents.JobCompleted(new JobId("done"), At)
            ]);

        var restored = (JobTrackerState)RoundTrip(state);

        Assert.Equal(state.PendingJobs, restored.PendingJobs);
        Assert.Equal(state.Jobs, restored.Jobs);
        Assert.Equal(state.Nodes, restored.Nodes);

        // The point of a snapshot: it has to be a usable starting state, not just equal data.
        Assert.Equal(new JobSize(60), restored.Nodes[NodeA].CapacityInUse);
        Assert.Equal(JobStatus.Running, restored.Jobs[new JobId("running")].Status);
    }

    [Fact]
    public void Refuses_types_it_does_not_own()
    {
        var serializer = new JobSchedulingSerializer((ExtendedActorSystem)Sys);

        // A silent JSON fallback for a persisted type is how journals become unreadable.
        Assert.Throws<ArgumentException>(() => serializer.Manifest("just a string"));
        Assert.Throws<ArgumentException>(() => serializer.FromBinary([0x90], "e:not-a-real-manifest"));
    }

    private object RoundTrip(object payload)
    {
        var serializer = (SerializerWithStringManifest)Sys.Serialization.FindSerializerFor(payload);
        return serializer.FromBinary(serializer.ToBinary(payload), serializer.Manifest(payload));
    }
}
