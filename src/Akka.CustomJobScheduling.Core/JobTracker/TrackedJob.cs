using Akka.Actor;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Everything the tracker knows about a single job: what it is, who asked for it, where it's
/// running, and how far along it is.
/// </summary>
/// <remarks>
/// Each state transition is a named method here rather than an inline <c>with</c> expression back in
/// <see cref="JobTrackerState"/>, so the rules about what a transition does to progress and node
/// assignment live next to the data they change.
/// </remarks>
public sealed record TrackedJob(
    JobDefinition Definition,
    JobSubmitterId SubmitterId,
    JobStatus Status,
    WorkProgress Progress,
    Address? AssignedNode,
    DateTimeOffset LastUpdatedAt) : IJobTrackerDomain, IWithJobId, IWithJobSubmitterId
{
    public JobId Id => Definition.Id;

    public JobSize Size => Definition.Size;

    /// <summary>True once the job can no longer change state.</summary>
    public bool IsTerminal =>
        Status is JobStatus.Completed or JobStatus.Faulted or JobStatus.Cancelled;

    public bool IsRunningOn(Address node) =>
        Status == JobStatus.Running && Equals(AssignedNode, node);

    /// <summary>A newly accepted job, waiting for a node.</summary>
    public static TrackedJob Waiting(
        JobDefinition definition,
        JobSubmitterId submitterId,
        DateTimeOffset acceptedAt) =>
        new(
            definition,
            submitterId,
            JobStatus.Waiting,
            WorkProgress.None(definition.Size),
            AssignedNode: null,
            acceptedAt);

    public TrackedJob RunOn(Address node, DateTimeOffset at) =>
        this with { Status = JobStatus.Running, AssignedNode = node, LastUpdatedAt = at };

    public TrackedJob WithProgress(WorkProgress progress, DateTimeOffset at) =>
        this with { Progress = progress, LastUpdatedAt = at };

    /// <summary>Finished successfully — by definition, all the way through its work.</summary>
    public TrackedJob Complete(DateTimeOffset at) =>
        Finish(JobStatus.Completed, WorkProgress.Full(Size), at);

    /// <summary>Failed, keeping whatever progress it had made.</summary>
    public TrackedJob Fail(DateTimeOffset at) => Finish(JobStatus.Faulted, Progress, at);

    /// <summary>Withdrawn by its submitter, keeping whatever progress it had made.</summary>
    public TrackedJob Cancel(DateTimeOffset at) => Finish(JobStatus.Cancelled, Progress, at);

    /// <summary>
    /// Back to waiting after losing its node. Whatever the lost node had done is gone with it.
    /// </summary>
    public TrackedJob Requeue(DateTimeOffset at) =>
        this with
        {
            Status = JobStatus.Waiting,
            Progress = WorkProgress.None(Size),
            AssignedNode = null,
            LastUpdatedAt = at
        };

    private TrackedJob Finish(JobStatus status, WorkProgress progress, DateTimeOffset at) =>
        this with
        {
            Status = status,
            Progress = progress,
            AssignedNode = null,
            LastUpdatedAt = at
        };

    /// <summary>The shape queries see.</summary>
    public JobProgress ToProgress() => new(Id, Status, Progress, LastUpdatedAt);

    public JobTrackerQueryResponses.JobStatusResult ToStatusResult() =>
        new(Id, SubmitterId, ToProgress(), AssignedNode);

    /// <summary>The shape subscribers and the submitter's shard region see.</summary>
    public JobTrackerNotifications.JobStatusChanged ToNotification() =>
        new(Id, SubmitterId, ToProgress(), AssignedNode);
}
