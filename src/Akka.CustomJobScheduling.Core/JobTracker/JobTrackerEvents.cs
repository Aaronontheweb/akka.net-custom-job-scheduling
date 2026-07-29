using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Facts recorded by the job tracker — what gets persisted and replayed on recovery.
/// </summary>
/// <remarks>
/// Every event carries its own <c>OccurredAt</c> instead of letting <see cref="JobTrackerState"/>
/// read the clock. That's what makes replay deterministic: the actor stamps the time once, and every
/// later recovery folds the exact same values.
/// </remarks>
public static class JobTrackerEvents
{
    // ---- cluster topology ----

    /// <summary>A node was added to the system with a given capacity.</summary>
    public sealed record NodeAdded(
        Address NodeAddress,
        MemberStatus Status,
        JobSize MaxCapacity,
        DateTimeOffset OccurredAt) : IJobTrackerEvent;

    /// <summary>
    /// A node was removed, forfeiting its capacity. Always emitted <i>after</i> the
    /// <see cref="JobRequeued"/> events for whatever it was running, so a replay never releases
    /// capacity on a node that's already gone from the map.
    /// </summary>
    public sealed record NodeRemoved(Address NodeAddress, DateTimeOffset OccurredAt)
        : IJobTrackerEvent;

    /// <summary>A known node changed <see cref="MemberStatus"/> or reachability.</summary>
    public sealed record NodeStatusChanged(
        Address NodeAddress,
        MemberStatus Status,
        bool Reachable,
        DateTimeOffset OccurredAt) : IJobTrackerEvent;

    // ---- job lifecycle ----

    /// <summary>A job was accepted into the queue.</summary>
    public sealed record JobAccepted(
        JobDefinition Job,
        JobSubmitterId SubmitterId,
        DateTimeOffset OccurredAt) : IJobTrackerEvent, IWithJobId, IWithJobSubmitterId
    {
        [System.Text.Json.Serialization.JsonIgnore]
        public JobId Id => Job.Id;
    }

    /// <summary>
    /// A job was assigned to a specific node's queue, either from the global queue or by a scale-out
    /// rebalance. It's committed to that node but not yet running — no capacity is consumed until
    /// <see cref="JobScheduled"/>.
    /// </summary>
    public sealed record JobQueued(JobId Id, Address NodeAddress, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>
    /// A queued job started running on its node, consuming that node's capacity.
    /// </summary>
    public sealed record JobScheduled(JobId Id, Address NodeAddress, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>A running job reported forward progress.</summary>
    public sealed record JobProgressed(JobId Id, WorkProgress Progress, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>A job finished successfully and released its capacity.</summary>
    public sealed record JobCompleted(JobId Id, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>A job failed and released its capacity.</summary>
    public sealed record JobFailed(JobId Id, string Reason, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>A job was cancelled by its submitter and released its capacity.</summary>
    public sealed record JobCancelled(JobId Id, string Reason, DateTimeOffset OccurredAt)
        : IJobTrackerEvent, IWithJobId;

    /// <summary>
    /// A running job lost its node and went back to the front of the queue. Explicit rather than
    /// folded into <see cref="NodeRemoved"/> so subscribers see a status change without needing to
    /// know anything about cluster topology.
    /// </summary>
    public sealed record JobRequeued(
        JobId Id,
        Address PreviousNode,
        string Reason,
        DateTimeOffset OccurredAt) : IJobTrackerEvent, IWithJobId;
}
