using System.Text.Json.Serialization;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// Source-generated System.Text.Json metadata for every type on the job scheduling wire.
/// </summary>
/// <remarks>
/// <para>
/// Metadata generation mode (not the fast-path) is deliberate: it lets the custom converters for
/// <c>Address</c>, <c>IActorRef</c>, and the value objects be resolved from the serializer's options
/// at runtime. The fast path would bypass them.
/// </para>
/// <para>
/// One <c>[JsonSerializable]</c> per concrete type we serialize. Collection and value types reached
/// through their properties come along automatically.
/// </para>
/// </remarks>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]

// events
[JsonSerializable(typeof(JobTrackerEvents.NodeAdded))]
[JsonSerializable(typeof(JobTrackerEvents.NodeRemoved))]
[JsonSerializable(typeof(JobTrackerEvents.NodeStatusChanged))]
[JsonSerializable(typeof(JobTrackerEvents.JobAccepted))]
[JsonSerializable(typeof(JobTrackerEvents.JobQueued))]
[JsonSerializable(typeof(JobTrackerEvents.JobScheduled))]
[JsonSerializable(typeof(JobTrackerEvents.JobProgressed))]
[JsonSerializable(typeof(JobTrackerEvents.JobCompleted))]
[JsonSerializable(typeof(JobTrackerEvents.JobFailed))]
[JsonSerializable(typeof(JobTrackerEvents.JobCancelled))]
[JsonSerializable(typeof(JobTrackerEvents.JobRequeued))]

// snapshot
[JsonSerializable(typeof(JobTrackerState))]

// commands
[JsonSerializable(typeof(JobTrackerCommands.SubmitJob))]
[JsonSerializable(typeof(JobTrackerCommands.CancelJob))]
[JsonSerializable(typeof(JobTrackerCommands.ReportProgress))]
[JsonSerializable(typeof(JobTrackerCommands.ReportJobCompleted))]
[JsonSerializable(typeof(JobTrackerCommands.ReportJobFailed))]
[JsonSerializable(typeof(JobTrackerCommands.NodeJoined))]
[JsonSerializable(typeof(JobTrackerCommands.NodeLeft))]
[JsonSerializable(typeof(JobTrackerCommands.NodeReachabilityChanged))]
[JsonSerializable(typeof(JobTrackerCommands.SyncNodes))]
[JsonSerializable(typeof(JobTrackerCommands.DrainQueue))]

// command responses
[JsonSerializable(typeof(JobTrackerResponses.CommandAccepted))]
[JsonSerializable(typeof(JobTrackerResponses.CommandRejected))]

// queries
[JsonSerializable(typeof(JobTrackerQueries.GetJobStatus))]
[JsonSerializable(typeof(JobTrackerQueries.SubscribeToJob))]
[JsonSerializable(typeof(JobTrackerQueries.UnsubscribeFromJob))]
[JsonSerializable(typeof(JobTrackerQueries.GetQueueStatus))]
[JsonSerializable(typeof(JobTrackerQueries.GetJobs))]
[JsonSerializable(typeof(JobTrackerQueries.SubscribeToQueue))]
[JsonSerializable(typeof(JobTrackerQueries.UnsubscribeFromQueue))]

// query responses
[JsonSerializable(typeof(JobTrackerQueryResponses.JobStatusResult))]
[JsonSerializable(typeof(JobTrackerQueryResponses.JobNotFound))]
[JsonSerializable(typeof(JobTrackerQueryResponses.SubscribeAck))]
[JsonSerializable(typeof(JobTrackerQueryResponses.UnsubscribeAck))]
[JsonSerializable(typeof(JobTrackerQueryResponses.QueueStatus))]
[JsonSerializable(typeof(JobTrackerQueryResponses.JobList))]

// notifications
[JsonSerializable(typeof(JobTrackerNotifications.JobStatusChanged))]

// execution protocol
[JsonSerializable(typeof(ExecutionMessages.ExecuteJob))]
[JsonSerializable(typeof(ExecutionMessages.CancelExecution))]
internal sealed partial class JobSchedulingJsonContext : JsonSerializerContext;
