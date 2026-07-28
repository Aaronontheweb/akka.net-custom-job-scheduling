using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Why a command could not be applied.
/// </summary>
/// <remarks>
/// Only commands are rejected. Facts (a worker report, a node leaving) are never refused — a stale
/// one simply produces no events — so there is no rejection reason for them here.
/// </remarks>
public enum JobRejectionReason
{
    DuplicateJobId = 0,
    UnknownJob = 1,
    AlreadyTerminal = 2,
    NotSubmitter = 3
}

/// <summary>
/// Commands for the job tracker: requests that are validated and may be rejected.
/// </summary>
/// <remarks>
/// The whole set is two. Everything the tracker also hears about — progress, completions, node
/// topology — has already happened and cannot be refused, so it lives in <see cref="JobTrackerFacts"/>
/// as an <see cref="IJobTrackerFact"/>, not here.
/// </remarks>
public static class JobTrackerCommands
{
    /// <summary>
    /// Submit a new job. Accepted jobs enter the queue and are placed as soon as a node is free.
    /// </summary>
    /// <remarks>
    /// The only reason to reject a submission is a duplicate id — a genuine conflict between two
    /// requests. A job larger than any node is <i>not</i> rejected: it's placed on the roomiest node
    /// and run there exclusively. Size is an execution-time concern, not a reason to refuse work.
    /// </remarks>
    public sealed record SubmitJob(JobDefinition Job, JobSubmitterId SubmitterId)
        : IJobTrackerCommand, IWithJobId, IWithJobSubmitterId
    {
        [System.Text.Json.Serialization.JsonIgnore]
        public JobId Id => Job.Id;
    }

    /// <summary>
    /// Cancel a job, releasing whatever capacity it holds. Only the original submitter may cancel.
    /// </summary>
    public sealed record CancelJob(JobId Id, JobSubmitterId SubmitterId, string Reason)
        : IJobTrackerCommand, IWithJobId, IWithJobSubmitterId;
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
