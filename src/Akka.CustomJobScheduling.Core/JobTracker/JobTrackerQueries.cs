using System.Collections.Immutable;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Read-only requests against the job tracker.
/// </summary>
/// <remarks>
/// Subscriptions are queries, not commands — subscribing changes no durable state. The set of
/// subscribers lives in the tracker <i>actor</i>, not in <see cref="JobTrackerState"/>: an
/// <see cref="IActorRef"/> is meaningless after a restart, so persisting one would be a lie.
/// </remarks>
public static class JobTrackerQueries
{
    /// <summary>Fetch the current status of a specific job.</summary>
    public sealed record GetJobStatus(JobId Id) : IJobTrackerQuery, IWithJobId;

    /// <summary>
    /// Subscribe to progress updates for a specific job. The subscriber gets the job's current
    /// status immediately, then a <see cref="JobTrackerNotifications.JobStatusChanged"/> per
    /// transition.
    /// </summary>
    public sealed record SubscribeToJob(JobId Id, IActorRef Subscriber)
        : IJobTrackerQuery, IWithJobId;

    /// <summary>Stop sending updates for a specific job.</summary>
    public sealed record UnsubscribeFromJob(JobId Id, IActorRef Subscriber)
        : IJobTrackerQuery, IWithJobId;

    /// <summary>Fetch the state of the queue and the cluster as a whole.</summary>
    public sealed record GetQueueStatus : IJobTrackerQuery
    {
        public static readonly GetQueueStatus Instance = new();
    }

    /// <summary>
    /// Fetch every job the tracker knows about, newest activity first.
    /// </summary>
    /// <param name="IncludeFinished">
    /// When false, only jobs that are still waiting or running come back.
    /// </param>
    /// <param name="Limit">Caps the result so a long-lived tracker can't return an unbounded page.</param>
    public sealed record GetJobs(bool IncludeFinished = true, int Limit = 200) : IJobTrackerQuery;

    /// <summary>
    /// Subscribe to <i>everything</i>: an opening snapshot, then every subsequent job transition
    /// and every change in cluster capacity.
    /// </summary>
    /// <remarks>
    /// Distinct from <see cref="SubscribeToJob"/>, which follows one job. This is what a dashboard
    /// wants — it has no id in hand and needs to learn about jobs it has never seen.
    /// </remarks>
    public sealed record SubscribeToQueue(IActorRef Subscriber) : IJobTrackerQuery;

    /// <summary>Stop sending queue-wide updates.</summary>
    public sealed record UnsubscribeFromQueue(IActorRef Subscriber) : IJobTrackerQuery;
}

/// <summary>
/// Replies to <see cref="JobTrackerQueries"/>.
/// </summary>
public static class JobTrackerQueryResponses
{
    public sealed record JobStatusResult(
        JobId Id,
        JobSubmitterId SubmitterId,
        JobProgress Progress,
        Address? AssignedNode) : IJobTrackerQueryResponse, IWithJobId, IWithJobSubmitterId;

    public sealed record JobNotFound(JobId Id) : IJobTrackerQueryResponse, IWithJobId;

    /// <summary>Answer to <see cref="JobTrackerQueries.GetJobs"/>.</summary>
    /// <param name="Jobs">Matching jobs, most recently updated first.</param>
    /// <param name="Total">How many matched before <c>Limit</c> was applied.</param>
    public sealed record JobList(ImmutableArray<JobStatusResult> Jobs, int Total)
        : IJobTrackerQueryResponse;

    public sealed record SubscribeAck(JobId Id, IActorRef Subscriber)
        : IJobTrackerQueryResponse, IWithJobId;

    public sealed record UnsubscribeAck(JobId Id, IActorRef Subscriber)
        : IJobTrackerQueryResponse, IWithJobId;

    /// <summary>
    /// A point-in-time snapshot of queue depth and cluster capacity.
    /// </summary>
    /// <param name="WaitingCount">Jobs accepted but not yet placed on a node.</param>
    /// <param name="RunningCount">Jobs currently placed on a node.</param>
    /// <param name="QueuedWork">Total <see cref="JobSize"/> of everything still waiting.</param>
    /// <param name="TotalCapacity">Sum of maximum capacity across all known nodes.</param>
    /// <param name="AvailableCapacity">Unused capacity across nodes eligible to accept work.</param>
    /// <param name="Nodes">Per-node detail, most available first.</param>
    public sealed record QueueStatus(
        int WaitingCount,
        int RunningCount,
        JobSize QueuedWork,
        JobSize TotalCapacity,
        JobSize AvailableCapacity,
        ImmutableArray<NodeStatus> Nodes) : IJobTrackerQueryResponse;
}

/// <summary>
/// Pushes sent to job subscribers and back to the original submitter.
/// </summary>
public static class JobTrackerNotifications
{
    /// <summary>
    /// A job changed state.
    /// </summary>
    /// <remarks>
    /// One type, delivered two ways: directly to every <see cref="IActorRef"/> that subscribed via
    /// <see cref="JobTrackerQueries.SubscribeToJob"/>, and through the submitter <c>ShardRegion</c>
    /// resolved from the Akka.Hosting <c>ActorRegistry</c>. It implements
    /// <see cref="IWithJobSubmitterId"/> precisely so the shard region's message extractor can route
    /// it without unwrapping — which is how we re-acquire submitters after a recovery that
    /// invalidated all their old <see cref="IActorRef"/>s.
    /// </remarks>
    public sealed record JobStatusChanged(
        JobId Id,
        JobSubmitterId SubmitterId,
        JobProgress Progress,
        Address? AssignedNode) : IJobTrackerNotification, IWithJobId, IWithJobSubmitterId;
}
