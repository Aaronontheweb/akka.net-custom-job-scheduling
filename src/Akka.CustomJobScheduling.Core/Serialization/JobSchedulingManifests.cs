namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// Stable wire identifiers for every serializable type in the job scheduling domain.
/// </summary>
/// <remarks>
/// <para>
/// These strings are written into the journal alongside every persisted event and are the only
/// thing that tells a future process how to read a record written today. They are therefore
/// <b>permanent</b>: renaming the C# type is free, changing the string here is a breaking change
/// that orphans every event already on disk.
/// </para>
/// <para>
/// That indirection is the whole point of a string-manifest serializer. Without it the manifest
/// defaults to the assembly-qualified type name, and moving a class between namespaces silently
/// breaks recovery.
/// </para>
/// </remarks>
public static class JobSchedulingManifests
{
    // Events — persisted. Never change these.
    public const string NodeAdded = "e:node-added";
    public const string NodeRemoved = "e:node-removed";
    public const string NodeStatusChanged = "e:node-status-changed";
    public const string JobAccepted = "e:job-accepted";
    public const string JobQueued = "e:job-queued";
    public const string JobScheduled = "e:job-scheduled";
    public const string JobProgressed = "e:job-progressed";
    public const string JobCompleted = "e:job-completed";
    public const string JobFailed = "e:job-failed";
    public const string JobCancelled = "e:job-cancelled";
    public const string JobRequeued = "e:job-requeued";

    // Snapshot — persisted.
    public const string TrackerState = "s:tracker-state";

    // Commands — cross-node.
    public const string SubmitJob = "c:submit-job";
    public const string CancelJob = "c:cancel-job";

    // Facts — cross-node. Not journaled, so these are wire identifiers only.
    public const string ProgressReported = "f:progress-reported";
    public const string ExecutionCompleted = "f:execution-completed";
    public const string ExecutionFailed = "f:execution-failed";
    public const string NodeJoined = "f:node-joined";
    public const string NodeLeft = "f:node-left";
    public const string NodeReachabilityChanged = "f:node-reachability";
    public const string NodesSynced = "f:nodes-synced";
    public const string DrainQueue = "f:drain-queue";

    // Command responses.
    public const string CommandAccepted = "r:accepted";
    public const string CommandRejected = "r:rejected";

    // Queries.
    public const string GetJobStatus = "q:get-job-status";
    public const string SubscribeToJob = "q:subscribe";
    public const string UnsubscribeFromJob = "q:unsubscribe";
    public const string GetQueueStatus = "q:get-queue-status";
    public const string GetJobs = "q:get-jobs";
    public const string SubscribeToQueue = "q:subscribe-queue";
    public const string UnsubscribeFromQueue = "q:unsubscribe-queue";

    // Query responses.
    public const string JobStatusResult = "qr:job-status";
    public const string JobNotFound = "qr:job-not-found";
    public const string SubscribeAck = "qr:subscribe-ack";
    public const string UnsubscribeAck = "qr:unsubscribe-ack";
    public const string QueueStatus = "qr:queue-status";
    public const string JobList = "qr:job-list";

    // Notifications.
    public const string JobStatusChanged = "n:job-status-changed";

    // Execution protocol.
    public const string ExecuteJob = "x:execute-job";
    public const string CancelExecution = "x:cancel-execution";
}
