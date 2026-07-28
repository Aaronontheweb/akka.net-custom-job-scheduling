using System.Text.Json;
using System.Text.Json.Serialization;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.Serialization;
using static Akka.CustomJobScheduling.Core.Serialization.JobSchedulingManifests;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// System.Text.Json serializer for the job scheduling domain, keyed by stable string manifests.
/// </summary>
/// <remarks>
/// <para>
/// Covers the same three concerns as before — events and snapshots for the journal, messages across
/// node boundaries, and their replies — but the wire format is now plain JSON produced by
/// source-generated metadata (<see cref="JobSchedulingJsonContext"/>) plus a handful of converters
/// for the Akka types and value objects. The payoff is legibility: a journal entry is readable JSON,
/// and the mapping from a C# record to its wire shape is whatever System.Text.Json does by default,
/// which most .NET developers already know.
/// </para>
/// <para>
/// The manifest strings and serializer id are unchanged from the previous MessagePack
/// implementation, but the bytes are not compatible: anything written by the old serializer must be
/// purged before this one reads the journal.
/// </para>
/// <para>
/// Deliberately absent: <c>JobStreamMessages</c>. Those carry a <c>ChannelReader</c> and a child
/// <see cref="IActorRef"/>, are in-process by construction, and must never travel — leaving them
/// unregistered makes an accidental remote send fail loudly.
/// </para>
/// </remarks>
public sealed class JobSchedulingSerializer : SerializerWithStringManifest
{
    /// <summary>
    /// Serializer id. Unique across the ActorSystem and stable — it is written into every persisted
    /// record.
    /// </summary>
    public const int Id = 8371;

    private readonly JsonSerializerOptions _options;

    public JobSchedulingSerializer(ExtendedActorSystem system) : base(system)
    {
        _options = new JsonSerializerOptions
        {
            TypeInfoResolver = JobSchedulingJsonContext.Default,
            Converters =
            {
                new AddressJsonConverter(),
                new ActorRefJsonConverter(system),
                new JobIdJsonConverter(),
                new JobSubmitterIdJsonConverter(),
                new JobSizeJsonConverter(),
                // Enums as their names rather than integers — a journal entry reads
                // "Running", not "1".
                new JsonStringEnumConverter()
            }
        };
    }

    public override int Identifier => Id;

    public override string Manifest(object o) => o switch
    {
        // events
        JobTrackerEvents.NodeAdded => NodeAdded,
        JobTrackerEvents.NodeRemoved => NodeRemoved,
        JobTrackerEvents.NodeStatusChanged => NodeStatusChanged,
        JobTrackerEvents.JobAccepted => JobAccepted,
        JobTrackerEvents.JobQueued => JobQueued,
        JobTrackerEvents.JobScheduled => JobScheduled,
        JobTrackerEvents.JobProgressed => JobProgressed,
        JobTrackerEvents.JobCompleted => JobCompleted,
        JobTrackerEvents.JobFailed => JobFailed,
        JobTrackerEvents.JobCancelled => JobCancelled,
        JobTrackerEvents.JobRequeued => JobRequeued,

        // snapshot
        JobTrackerState => TrackerState,

        // commands
        JobTrackerCommands.SubmitJob => SubmitJob,
        JobTrackerCommands.CancelJob => CancelJob,

        // facts
        JobTrackerFacts.ProgressReported => ProgressReported,
        JobTrackerFacts.ExecutionCompleted => ExecutionCompleted,
        JobTrackerFacts.ExecutionFailed => ExecutionFailed,
        JobTrackerFacts.NodeJoined => NodeJoined,
        JobTrackerFacts.NodeLeft => NodeLeft,
        JobTrackerFacts.NodeReachabilityChanged => NodeReachabilityChanged,
        JobTrackerFacts.NodesSynced => NodesSynced,
        JobTrackerFacts.DrainQueue => DrainQueue,

        // command responses
        JobTrackerResponses.CommandAccepted => CommandAccepted,
        JobTrackerResponses.CommandRejected => CommandRejected,

        // queries
        JobTrackerQueries.GetJobStatus => GetJobStatus,
        JobTrackerQueries.SubscribeToJob => SubscribeToJob,
        JobTrackerQueries.UnsubscribeFromJob => UnsubscribeFromJob,
        JobTrackerQueries.GetQueueStatus => GetQueueStatus,
        JobTrackerQueries.GetJobs => GetJobs,
        JobTrackerQueries.SubscribeToQueue => SubscribeToQueue,
        JobTrackerQueries.UnsubscribeFromQueue => UnsubscribeFromQueue,

        // query responses
        JobTrackerQueryResponses.JobStatusResult => JobStatusResult,
        JobTrackerQueryResponses.JobNotFound => JobNotFound,
        JobTrackerQueryResponses.SubscribeAck => SubscribeAck,
        JobTrackerQueryResponses.UnsubscribeAck => UnsubscribeAck,
        JobTrackerQueryResponses.QueueStatus => QueueStatus,
        JobTrackerQueryResponses.JobList => JobList,

        // notifications
        JobTrackerNotifications.JobStatusChanged => JobStatusChanged,

        // execution protocol
        ExecutionMessages.ExecuteJob => ExecuteJob,
        ExecutionMessages.CancelExecution => CancelExecution,

        _ => throw new ArgumentException(
            $"{nameof(JobSchedulingSerializer)} cannot serialize {o.GetType().FullName}.",
            nameof(o))
    };

    public override byte[] ToBinary(object obj) =>
        JsonSerializer.SerializeToUtf8Bytes(obj, obj.GetType(), _options);

    public override object FromBinary(byte[] bytes, string manifest)
    {
        var type = TypeForManifest(manifest);

        return JsonSerializer.Deserialize(bytes, type, _options)
               ?? throw new ArgumentException(
                   $"{nameof(JobSchedulingSerializer)} deserialized manifest '{manifest}' to null.",
                   nameof(bytes));
    }

    private static Type TypeForManifest(string manifest) => manifest switch
    {
        NodeAdded => typeof(JobTrackerEvents.NodeAdded),
        NodeRemoved => typeof(JobTrackerEvents.NodeRemoved),
        NodeStatusChanged => typeof(JobTrackerEvents.NodeStatusChanged),
        JobAccepted => typeof(JobTrackerEvents.JobAccepted),
        JobQueued => typeof(JobTrackerEvents.JobQueued),
        JobScheduled => typeof(JobTrackerEvents.JobScheduled),
        JobProgressed => typeof(JobTrackerEvents.JobProgressed),
        JobCompleted => typeof(JobTrackerEvents.JobCompleted),
        JobFailed => typeof(JobTrackerEvents.JobFailed),
        JobCancelled => typeof(JobTrackerEvents.JobCancelled),
        JobRequeued => typeof(JobTrackerEvents.JobRequeued),

        TrackerState => typeof(JobTrackerState),

        SubmitJob => typeof(JobTrackerCommands.SubmitJob),
        CancelJob => typeof(JobTrackerCommands.CancelJob),
        ProgressReported => typeof(JobTrackerFacts.ProgressReported),
        ExecutionCompleted => typeof(JobTrackerFacts.ExecutionCompleted),
        ExecutionFailed => typeof(JobTrackerFacts.ExecutionFailed),
        NodeJoined => typeof(JobTrackerFacts.NodeJoined),
        NodeLeft => typeof(JobTrackerFacts.NodeLeft),
        NodeReachabilityChanged => typeof(JobTrackerFacts.NodeReachabilityChanged),
        NodesSynced => typeof(JobTrackerFacts.NodesSynced),
        DrainQueue => typeof(JobTrackerFacts.DrainQueue),

        CommandAccepted => typeof(JobTrackerResponses.CommandAccepted),
        CommandRejected => typeof(JobTrackerResponses.CommandRejected),

        GetJobStatus => typeof(JobTrackerQueries.GetJobStatus),
        SubscribeToJob => typeof(JobTrackerQueries.SubscribeToJob),
        UnsubscribeFromJob => typeof(JobTrackerQueries.UnsubscribeFromJob),
        GetQueueStatus => typeof(JobTrackerQueries.GetQueueStatus),
        GetJobs => typeof(JobTrackerQueries.GetJobs),
        SubscribeToQueue => typeof(JobTrackerQueries.SubscribeToQueue),
        UnsubscribeFromQueue => typeof(JobTrackerQueries.UnsubscribeFromQueue),

        JobStatusResult => typeof(JobTrackerQueryResponses.JobStatusResult),
        JobNotFound => typeof(JobTrackerQueryResponses.JobNotFound),
        SubscribeAck => typeof(JobTrackerQueryResponses.SubscribeAck),
        UnsubscribeAck => typeof(JobTrackerQueryResponses.UnsubscribeAck),
        QueueStatus => typeof(JobTrackerQueryResponses.QueueStatus),
        JobList => typeof(JobTrackerQueryResponses.JobList),

        JobStatusChanged => typeof(JobTrackerNotifications.JobStatusChanged),

        ExecuteJob => typeof(ExecutionMessages.ExecuteJob),
        CancelExecution => typeof(ExecutionMessages.CancelExecution),

        _ => throw new ArgumentException(
            $"{nameof(JobSchedulingSerializer)} has no type for manifest '{manifest}'.",
            nameof(manifest))
    };
}
