using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Facts reported to the tracker from outside: how a running job is doing, and how cluster
/// membership is changing. None of these can be rejected — they describe something that already
/// happened. <see cref="JobTrackerState.Integrate"/> folds each into the events it implies.
/// </summary>
public static class JobTrackerFacts
{
    // ---- worker execution ----

    /// <summary>A worker node reporting how far it has gotten through a running job.</summary>
    public sealed record ProgressReported(JobId Id, Address NodeAddress, WorkProgress Progress)
        : IJobTrackerFact, IWithJobId;

    /// <summary>A worker node reporting that a job finished successfully.</summary>
    public sealed record ExecutionCompleted(JobId Id, Address NodeAddress)
        : IJobTrackerFact, IWithJobId;

    /// <summary>A worker node reporting that a job failed.</summary>
    public sealed record ExecutionFailed(JobId Id, Address NodeAddress, string Reason)
        : IJobTrackerFact, IWithJobId;

    // ---- cluster membership ----

    /// <summary>
    /// A node became available to run work. Derived by the tracker actor from
    /// <see cref="ClusterEvent.IMemberEvent"/> plus the default capacity from configuration.
    /// </summary>
    /// <remarks>
    /// A re-announcement of a known node (say <c>WeaklyUp</c> -> <c>Up</c>) is not a new node:
    /// <see cref="JobTrackerState.Integrate"/> folds it to a status change rather than discarding the
    /// capacity the node has already committed to.
    /// </remarks>
    public sealed record NodeJoined(Address NodeAddress, MemberStatus Status, JobSize MaxCapacity)
        : IJobTrackerFact;

    /// <summary>
    /// A node is gone for good. Everything it was running gets requeued.
    /// </summary>
    /// <remarks>
    /// Derive this from <see cref="ClusterEvent.MemberRemoved"/> — the point at which Akka.Cluster
    /// has committed to the node being dead — not from <see cref="ClusterEvent.UnreachableMember"/>.
    /// See the comment on <c>JobTrackerState.OnNodeReachabilityChanged</c> for why the distinction
    /// matters.
    /// </remarks>
    public sealed record NodeLeft(Address NodeAddress) : IJobTrackerFact;

    /// <summary>
    /// Akka.Cluster's failure detector changed its mind about whether it can see a node. Derived
    /// from <see cref="ClusterEvent.UnreachableMember"/> and <see cref="ClusterEvent.ReachableMember"/>.
    /// </summary>
    /// <remarks>
    /// Unreachable nodes stop receiving new work but keep what they already hold. This is a
    /// transient verdict: it either heals, or the downing provider escalates it to
    /// <see cref="ClusterEvent.MemberRemoved"/> and <see cref="NodeLeft"/> does the requeueing.
    /// </remarks>
    public sealed record NodeReachabilityChanged(Address NodeAddress, bool Reachable)
        : IJobTrackerFact;

    /// <summary>
    /// The authoritative set of worker nodes, as a whole. Anything the tracker believes in that
    /// isn't listed here has left; anything listed that it doesn't know about is new.
    /// </summary>
    /// <remarks>
    /// Derived from <see cref="ClusterEvent.CurrentClusterState"/>, which arrives on subscribe.
    /// Incremental facts alone can't recover a tracker that was down while a node left: the
    /// <see cref="ClusterEvent.MemberRemoved"/> went to a process that no longer exists, and
    /// replaying the journal faithfully restores a node that is gone. Reconciling against the live
    /// membership is what closes that gap — the largest fan-out in the system, one snapshot becoming
    /// a removal-plus-requeue for every departed node and an add for every new one.
    /// </remarks>
    /// <param name="Members">Every worker node the cluster currently has, with its capacity.</param>
    public sealed record NodesSynced(ImmutableDictionary<Address, JobSize> Members) : IJobTrackerFact;

    /// <summary>
    /// Internal tick: try to place queued jobs. Not an external fact, but it shares the fact path —
    /// it produces placement events and can never be rejected. Every command and fact already drains
    /// the queue, so this is just a safety net for a tracker that has gone quiet with work still
    /// queued.
    /// </summary>
    public sealed record DrainQueue : IJobTrackerFact
    {
        public static readonly DrainQueue Instance = new();
    }
}
