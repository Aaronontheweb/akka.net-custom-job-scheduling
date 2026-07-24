using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Why a command could not be applied.
/// </summary>
public enum JobRejectionReason
{
    DuplicateJobId = 0,
    UnknownJob = 1,
    AlreadyTerminal = 2,
    NotSubmitter = 3,

    /// <summary>No node in the cluster is large enough to ever run this job.</summary>
    ExceedsClusterCapacity = 4,

    /// <summary>A report arrived from a node that no longer owns the job.</summary>
    StaleReport = 5
}

/// <summary>
/// Commands for the job tracker.
/// </summary>
public static class JobTrackerCommands
{
    /// <summary>
    /// Submit a new job. Accepted jobs enter the queue and are placed as soon as capacity exists.
    /// </summary>
    public sealed record SubmitJob(JobDefinition Job, JobSubmitterId SubmitterId)
        : IJobTrackerCommand, IWithJobId, IWithJobSubmitterId
    {
        public JobId Id => Job.Id;
    }

    /// <summary>
    /// Cancel a job, releasing whatever capacity it holds. Only the original submitter may cancel.
    /// </summary>
    public sealed record CancelJob(JobId Id, JobSubmitterId SubmitterId, string Reason)
        : IJobTrackerCommand, IWithJobId, IWithJobSubmitterId;

    /// <summary>A worker node reporting how far it has gotten through a running job.</summary>
    public sealed record ReportProgress(JobId Id, Address NodeAddress, WorkProgress Progress)
        : IJobTrackerCommand, IWithJobId;

    /// <summary>A worker node reporting that a job finished successfully.</summary>
    public sealed record ReportJobCompleted(JobId Id, Address NodeAddress)
        : IJobTrackerCommand, IWithJobId;

    /// <summary>A worker node reporting that a job failed.</summary>
    public sealed record ReportJobFailed(JobId Id, Address NodeAddress, string Reason)
        : IJobTrackerCommand, IWithJobId;

    /// <summary>
    /// A node became available to run work. Derived by the tracker actor from
    /// <see cref="ClusterEvent.IMemberEvent"/> plus the default capacity from configuration.
    /// </summary>
    /// <remarks>
    /// A command rather than an event because the tracker still gets to decide: a re-announcement of
    /// a known node updates its status instead of resetting the capacity it's already committed to.
    /// </remarks>
    public sealed record NodeJoined(Address NodeAddress, MemberStatus Status, JobSize MaxCapacity)
        : IJobTrackerCommand;

    /// <summary>
    /// A node is gone for good. Everything it was running gets requeued.
    /// </summary>
    /// <remarks>
    /// Derive this from <see cref="ClusterEvent.MemberRemoved"/> — the point at which Akka.Cluster
    /// has committed to the node being dead — not from
    /// <see cref="ClusterEvent.UnreachableMember"/>. See the comment on
    /// <c>JobTrackerState.OnNodeReachabilityChanged</c> for why the distinction matters.
    /// </remarks>
    public sealed record NodeLeft(Address NodeAddress) : IJobTrackerCommand;

    /// <summary>
    /// Akka.Cluster's failure detector changed its mind about whether it can see a node. Derived
    /// from <see cref="ClusterEvent.UnreachableMember"/> and
    /// <see cref="ClusterEvent.ReachableMember"/>.
    /// </summary>
    /// <remarks>
    /// Unreachable nodes stop receiving new work but keep what they already hold. This is a
    /// transient verdict: it either heals, or the downing provider escalates it to
    /// <see cref="ClusterEvent.MemberRemoved"/> and <see cref="NodeLeft"/> does the requeueing.
    /// </remarks>
    public sealed record NodeReachabilityChanged(Address NodeAddress, bool Reachable)
        : IJobTrackerCommand;

    /// <summary>
    /// Internal tick: try to place queued jobs. Every other command already drains the queue, so
    /// this is just a safety net for a tracker that has gone quiet with work still queued.
    /// </summary>
    public sealed record DrainQueue : IJobTrackerCommand
    {
        public static readonly DrainQueue Instance = new();
    }
}

/// <summary>
/// Acknowledgements returned to the sender of an <see cref="IJobTrackerCommand"/>.
/// </summary>
public static class JobTrackerResponses
{
    public sealed record CommandAccepted(JobId Id) : IJobTrackerCommandResponse, IWithJobId;

    public sealed record CommandRejected(JobId Id, JobRejectionReason Reason, string Message)
        : IJobTrackerCommandResponse, IWithJobId;
}
