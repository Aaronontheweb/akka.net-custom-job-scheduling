using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// The protocol between the tracker and the worker nodes it places jobs on.
/// </summary>
/// <remarks>
/// Kept separate from <see cref="JobTrackerCommands"/> because this travels the other direction:
/// these are instructions the tracker issues, not requests it receives.
/// </remarks>
public static class ExecutionMessages
{
    /// <summary>
    /// Run this job. Sent to the <see cref="JobReceiverActor"/> on the node the tracker placed it on.
    /// </summary>
    public sealed record ExecuteJob(JobDefinition Job) : IWithJobId
    {
        public JobId Id => Job.Id;
    }

    /// <summary>
    /// Stop running this job, if it still is. Idempotent — a receiver that has never heard of the
    /// job ignores it.
    /// </summary>
    public sealed record CancelExecution(JobId Id, string Reason) : IWithJobId;
}
