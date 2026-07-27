using System.Buffers;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Serialization;
using MessagePack;
using static Akka.CustomJobScheduling.Core.Serialization.JobSchedulingManifests;
using static Akka.CustomJobScheduling.Core.Serialization.WireFormat;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// MessagePack serializer for the job scheduling domain, keyed by stable string manifests.
/// </summary>
/// <remarks>
/// <para>
/// Covers three separate concerns that happen to share machinery: events and snapshots written to
/// the journal, messages crossing node boundaries, and replies coming back. Without it Akka falls
/// back to the Newtonsoft JSON serializer, which works but writes type names into the journal and
/// is markedly slower.
/// </para>
/// <para>
/// Deliberately absent: <c>JobStreamMessages</c>. Those carry a <c>ChannelReader</c> and a child
/// <see cref="IActorRef"/>, are in-process by construction, and must never acquire the ability to
/// travel — leaving them unregistered means an accidental remote send fails loudly.
/// </para>
/// </remarks>
public sealed class JobSchedulingSerializer : SerializerWithStringManifest
{
    /// <summary>
    /// Serializer id. Must be unique across the ActorSystem and stable forever — it is written
    /// into every persisted record.
    /// </summary>
    public const int Id = 8371;

    private readonly ExtendedActorSystem _system;

    public JobSchedulingSerializer(ExtendedActorSystem system) : base(system)
    {
        _system = system;
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
        JobTrackerCommands.ReportProgress => ReportProgress,
        JobTrackerCommands.ReportJobCompleted => ReportJobCompleted,
        JobTrackerCommands.ReportJobFailed => ReportJobFailed,
        JobTrackerCommands.NodeJoined => NodeJoined,
        JobTrackerCommands.NodeLeft => NodeLeft,
        JobTrackerCommands.NodeReachabilityChanged => NodeReachabilityChanged,
        JobTrackerCommands.SyncNodes => SyncNodes,
        JobTrackerCommands.DrainQueue => DrainQueue,

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

    public override byte[] ToBinary(object obj)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);

        WriteBody(ref writer, obj);

        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    public override object FromBinary(byte[] bytes, string manifest)
    {
        var reader = new MessagePackReader(bytes);
        return ReadBody(ref reader, manifest);
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    private void WriteBody(ref MessagePackWriter w, object obj)
    {
        switch (obj)
        {
            // ---- events ----
            case JobTrackerEvents.NodeAdded e:
                w.WriteArrayHeader(4);
                Write(ref w, e.NodeAddress);
                Write(ref w, e.Status);
                Write(ref w, e.MaxCapacity);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.NodeRemoved e:
                w.WriteArrayHeader(2);
                Write(ref w, e.NodeAddress);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.NodeStatusChanged e:
                w.WriteArrayHeader(4);
                Write(ref w, e.NodeAddress);
                Write(ref w, e.Status);
                w.Write(e.Reachable);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobAccepted e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Job);
                Write(ref w, e.SubmitterId);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobQueued e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Id);
                Write(ref w, e.NodeAddress);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobScheduled e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Id);
                Write(ref w, e.NodeAddress);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobProgressed e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Id);
                Write(ref w, e.Progress);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobCompleted e:
                w.WriteArrayHeader(2);
                Write(ref w, e.Id);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobFailed e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Id);
                w.Write(e.Reason);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobCancelled e:
                w.WriteArrayHeader(3);
                Write(ref w, e.Id);
                w.Write(e.Reason);
                Write(ref w, e.OccurredAt);
                break;

            case JobTrackerEvents.JobRequeued e:
                w.WriteArrayHeader(4);
                Write(ref w, e.Id);
                Write(ref w, e.PreviousNode);
                w.Write(e.Reason);
                Write(ref w, e.OccurredAt);
                break;

            // ---- snapshot ----
            case JobTrackerState state:
                WriteState(ref w, state);
                break;

            // ---- commands ----
            case JobTrackerCommands.SubmitJob c:
                w.WriteArrayHeader(2);
                Write(ref w, c.Job);
                Write(ref w, c.SubmitterId);
                break;

            case JobTrackerCommands.CancelJob c:
                w.WriteArrayHeader(3);
                Write(ref w, c.Id);
                Write(ref w, c.SubmitterId);
                w.Write(c.Reason);
                break;

            case JobTrackerCommands.ReportProgress c:
                w.WriteArrayHeader(3);
                Write(ref w, c.Id);
                Write(ref w, c.NodeAddress);
                Write(ref w, c.Progress);
                break;

            case JobTrackerCommands.ReportJobCompleted c:
                w.WriteArrayHeader(2);
                Write(ref w, c.Id);
                Write(ref w, c.NodeAddress);
                break;

            case JobTrackerCommands.ReportJobFailed c:
                w.WriteArrayHeader(3);
                Write(ref w, c.Id);
                Write(ref w, c.NodeAddress);
                w.Write(c.Reason);
                break;

            case JobTrackerCommands.NodeJoined c:
                w.WriteArrayHeader(3);
                Write(ref w, c.NodeAddress);
                Write(ref w, c.Status);
                Write(ref w, c.MaxCapacity);
                break;

            case JobTrackerCommands.NodeLeft c:
                w.WriteArrayHeader(1);
                Write(ref w, c.NodeAddress);
                break;

            case JobTrackerCommands.NodeReachabilityChanged c:
                w.WriteArrayHeader(2);
                Write(ref w, c.NodeAddress);
                w.Write(c.Reachable);
                break;

            case JobTrackerCommands.SyncNodes c:
                w.WriteArrayHeader(1);
                w.WriteMapHeader(c.Members.Count);
                foreach (var (address, capacity) in c.Members)
                {
                    Write(ref w, address);
                    Write(ref w, capacity);
                }

                break;

            case JobTrackerCommands.DrainQueue:
                w.WriteArrayHeader(0);
                break;

            // ---- command responses ----
            case JobTrackerResponses.CommandAccepted r:
                w.WriteArrayHeader(1);
                Write(ref w, r.Id);
                break;

            case JobTrackerResponses.CommandRejected r:
                w.WriteArrayHeader(3);
                Write(ref w, r.Id);
                w.Write((int)r.Reason);
                w.Write(r.Message);
                break;

            // ---- queries ----
            case JobTrackerQueries.GetJobStatus q:
                w.WriteArrayHeader(1);
                Write(ref w, q.Id);
                break;

            case JobTrackerQueries.SubscribeToJob q:
                w.WriteArrayHeader(2);
                Write(ref w, q.Id);
                Write(ref w, q.Subscriber);
                break;

            case JobTrackerQueries.UnsubscribeFromJob q:
                w.WriteArrayHeader(2);
                Write(ref w, q.Id);
                Write(ref w, q.Subscriber);
                break;

            case JobTrackerQueries.GetQueueStatus:
                w.WriteArrayHeader(0);
                break;

            case JobTrackerQueries.GetJobs q:
                w.WriteArrayHeader(2);
                w.Write(q.IncludeFinished);
                w.Write(q.Limit);
                break;

            case JobTrackerQueries.SubscribeToQueue q:
                w.WriteArrayHeader(1);
                Write(ref w, q.Subscriber);
                break;

            case JobTrackerQueries.UnsubscribeFromQueue q:
                w.WriteArrayHeader(1);
                Write(ref w, q.Subscriber);
                break;

            // ---- query responses ----
            case JobTrackerQueryResponses.JobStatusResult q:
                w.WriteArrayHeader(6);
                Write(ref w, q.Id);
                Write(ref w, q.SubmitterId);
                Write(ref w, q.Progress);
                Write(ref w, q.AssignedNode);
                Write(ref w, q.SubmittedAt);
                Write(ref w, q.StartedAt);
                break;

            case JobTrackerQueryResponses.JobNotFound q:
                w.WriteArrayHeader(1);
                Write(ref w, q.Id);
                break;

            case JobTrackerQueryResponses.JobList q:
                w.WriteArrayHeader(2);
                w.Write(q.Total);
                w.WriteArrayHeader(q.Jobs.Length);
                foreach (var job in q.Jobs)
                {
                    WriteBody(ref w, job);
                }

                break;

            case JobTrackerQueryResponses.SubscribeAck q:
                w.WriteArrayHeader(2);
                Write(ref w, q.Id);
                Write(ref w, q.Subscriber);
                break;

            case JobTrackerQueryResponses.UnsubscribeAck q:
                w.WriteArrayHeader(2);
                Write(ref w, q.Id);
                Write(ref w, q.Subscriber);
                break;

            case JobTrackerQueryResponses.QueueStatus q:
                w.WriteArrayHeader(7);
                w.Write(q.WaitingCount);
                w.Write(q.RunningCount);
                Write(ref w, q.QueuedWork);
                Write(ref w, q.TotalCapacity);
                Write(ref w, q.AvailableCapacity);
                w.WriteArrayHeader(q.Nodes.Length);
                foreach (var node in q.Nodes)
                {
                    Write(ref w, node);
                }

                w.Write(q.QueuedCount);
                break;

            // ---- notifications ----
            case JobTrackerNotifications.JobStatusChanged n:
                w.WriteArrayHeader(6);
                Write(ref w, n.Id);
                Write(ref w, n.SubmitterId);
                Write(ref w, n.Progress);
                Write(ref w, n.AssignedNode);
                Write(ref w, n.SubmittedAt);
                Write(ref w, n.StartedAt);
                break;

            // ---- execution protocol ----
            case ExecutionMessages.ExecuteJob x:
                w.WriteArrayHeader(1);
                Write(ref w, x.Job);
                break;

            case ExecutionMessages.CancelExecution x:
                w.WriteArrayHeader(2);
                Write(ref w, x.Id);
                w.Write(x.Reason);
                break;

            default:
                throw new ArgumentException(
                    $"{nameof(JobSchedulingSerializer)} cannot serialize {obj.GetType().FullName}.",
                    nameof(obj));
        }
    }

    private static void WriteState(ref MessagePackWriter w, JobTrackerState state)
    {
        w.WriteArrayHeader(3);

        w.WriteArrayHeader(state.PendingJobs.Count);
        foreach (var id in state.PendingJobs)
        {
            Write(ref w, id);
        }

        w.WriteArrayHeader(state.Jobs.Count);
        foreach (var job in state.Jobs.Values)
        {
            Write(ref w, job);
        }

        w.WriteArrayHeader(state.Nodes.Count);
        foreach (var node in state.Nodes.Values)
        {
            Write(ref w, node);
        }
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    private object ReadBody(ref MessagePackReader r, string manifest)
    {
        var fields = r.ReadArrayHeader();

        switch (manifest)
        {
            case NodeAdded:
            {
                var address = ReadAddress(ref r);
                var status = ReadMemberStatus(ref r);
                var capacity = ReadJobSize(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 4);
                return new JobTrackerEvents.NodeAdded(address, status, capacity, at);
            }

            case NodeRemoved:
            {
                var address = ReadAddress(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 2);
                return new JobTrackerEvents.NodeRemoved(address, at);
            }

            case NodeStatusChanged:
            {
                var address = ReadAddress(ref r);
                var status = ReadMemberStatus(ref r);
                var reachable = r.ReadBoolean();
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 4);
                return new JobTrackerEvents.NodeStatusChanged(address, status, reachable, at);
            }

            case JobAccepted:
            {
                var job = ReadJobDefinition(ref r);
                var submitter = ReadSubmitterId(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobAccepted(job, submitter, at);
            }

            case JobQueued:
            {
                var id = ReadJobId(ref r);
                var address = ReadAddress(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobQueued(id, address, at);
            }

            case JobScheduled:
            {
                var id = ReadJobId(ref r);
                var address = ReadAddress(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobScheduled(id, address, at);
            }

            case JobProgressed:
            {
                var id = ReadJobId(ref r);
                var progress = ReadWorkProgress(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobProgressed(id, progress, at);
            }

            case JobCompleted:
            {
                var id = ReadJobId(ref r);
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 2);
                return new JobTrackerEvents.JobCompleted(id, at);
            }

            case JobFailed:
            {
                var id = ReadJobId(ref r);
                var reason = r.ReadString() ?? string.Empty;
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobFailed(id, reason, at);
            }

            case JobCancelled:
            {
                var id = ReadJobId(ref r);
                var reason = r.ReadString() ?? string.Empty;
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerEvents.JobCancelled(id, reason, at);
            }

            case JobRequeued:
            {
                var id = ReadJobId(ref r);
                var previous = ReadAddress(ref r);
                var reason = r.ReadString() ?? string.Empty;
                var at = ReadTimestamp(ref r);
                Skip(ref r, fields, 4);
                return new JobTrackerEvents.JobRequeued(id, previous, reason, at);
            }

            case TrackerState:
                return ReadState(ref r, fields);

            case SubmitJob:
            {
                var job = ReadJobDefinition(ref r);
                var submitter = ReadSubmitterId(ref r);
                Skip(ref r, fields, 2);
                return new JobTrackerCommands.SubmitJob(job, submitter);
            }

            case CancelJob:
            {
                var id = ReadJobId(ref r);
                var submitter = ReadSubmitterId(ref r);
                var reason = r.ReadString() ?? string.Empty;
                Skip(ref r, fields, 3);
                return new JobTrackerCommands.CancelJob(id, submitter, reason);
            }

            case ReportProgress:
            {
                var id = ReadJobId(ref r);
                var address = ReadAddress(ref r);
                var progress = ReadWorkProgress(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerCommands.ReportProgress(id, address, progress);
            }

            case ReportJobCompleted:
            {
                var id = ReadJobId(ref r);
                var address = ReadAddress(ref r);
                Skip(ref r, fields, 2);
                return new JobTrackerCommands.ReportJobCompleted(id, address);
            }

            case ReportJobFailed:
            {
                var id = ReadJobId(ref r);
                var address = ReadAddress(ref r);
                var reason = r.ReadString() ?? string.Empty;
                Skip(ref r, fields, 3);
                return new JobTrackerCommands.ReportJobFailed(id, address, reason);
            }

            case NodeJoined:
            {
                var address = ReadAddress(ref r);
                var status = ReadMemberStatus(ref r);
                var capacity = ReadJobSize(ref r);
                Skip(ref r, fields, 3);
                return new JobTrackerCommands.NodeJoined(address, status, capacity);
            }

            case NodeLeft:
            {
                var address = ReadAddress(ref r);
                Skip(ref r, fields, 1);
                return new JobTrackerCommands.NodeLeft(address);
            }

            case NodeReachabilityChanged:
            {
                var address = ReadAddress(ref r);
                var reachable = r.ReadBoolean();
                Skip(ref r, fields, 2);
                return new JobTrackerCommands.NodeReachabilityChanged(address, reachable);
            }

            case SyncNodes:
            {
                var count = r.ReadMapHeader();
                var members = ImmutableDictionary.CreateBuilder<Address, JobSize>();
                for (var i = 0; i < count; i++)
                {
                    var address = ReadAddress(ref r);
                    members.Add(address, ReadJobSize(ref r));
                }

                Skip(ref r, fields, 1);
                return new JobTrackerCommands.SyncNodes(members.ToImmutable());
            }

            case DrainQueue:
                Skip(ref r, fields, 0);
                return JobTrackerCommands.DrainQueue.Instance;

            case CommandAccepted:
            {
                var id = ReadJobId(ref r);
                Skip(ref r, fields, 1);
                return new JobTrackerResponses.CommandAccepted(id);
            }

            case CommandRejected:
            {
                var id = ReadJobId(ref r);
                var reason = (JobRejectionReason)r.ReadInt32();
                var message = r.ReadString() ?? string.Empty;
                Skip(ref r, fields, 3);
                return new JobTrackerResponses.CommandRejected(id, reason, message);
            }

            case GetJobStatus:
            {
                var id = ReadJobId(ref r);
                Skip(ref r, fields, 1);
                return new JobTrackerQueries.GetJobStatus(id);
            }

            case SubscribeToJob:
            {
                var id = ReadJobId(ref r);
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 2);
                return new JobTrackerQueries.SubscribeToJob(id, subscriber);
            }

            case UnsubscribeFromJob:
            {
                var id = ReadJobId(ref r);
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 2);
                return new JobTrackerQueries.UnsubscribeFromJob(id, subscriber);
            }

            case GetQueueStatus:
                Skip(ref r, fields, 0);
                return JobTrackerQueries.GetQueueStatus.Instance;

            case GetJobs:
            {
                var includeFinished = r.ReadBoolean();
                var limit = r.ReadInt32();
                Skip(ref r, fields, 2);
                return new JobTrackerQueries.GetJobs(includeFinished, limit);
            }

            case SubscribeToQueue:
            {
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 1);
                return new JobTrackerQueries.SubscribeToQueue(subscriber);
            }

            case UnsubscribeFromQueue:
            {
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 1);
                return new JobTrackerQueries.UnsubscribeFromQueue(subscriber);
            }

            case JobList:
            {
                var total = r.ReadInt32();
                var count = r.ReadArrayHeader();
                var jobs = ImmutableArray.CreateBuilder<JobTrackerQueryResponses.JobStatusResult>(count);
                for (var i = 0; i < count; i++)
                {
                    jobs.Add((JobTrackerQueryResponses.JobStatusResult)ReadBody(ref r, JobStatusResult));
                }

                Skip(ref r, fields, 2);
                return new JobTrackerQueryResponses.JobList(jobs.ToImmutable(), total);
            }

            case JobStatusResult:
            {
                var id = ReadJobId(ref r);
                var submitter = ReadSubmitterId(ref r);
                var progress = ReadJobProgress(ref r);
                var assigned = ReadAddressOrNull(ref r);

                // Appended after the first release; a sender on the old schema stops at four.
                var submittedAt = progress.LastUpdatedAt;
                if (fields > 4)
                    submittedAt = ReadTimestamp(ref r);

                DateTimeOffset? startedAt = null;
                if (fields > 5)
                    startedAt = ReadTimestampOrNull(ref r);

                Skip(ref r, fields, 6);
                return new JobTrackerQueryResponses.JobStatusResult(
                    id, submitter, progress, assigned, submittedAt, startedAt);
            }

            case JobNotFound:
            {
                var id = ReadJobId(ref r);
                Skip(ref r, fields, 1);
                return new JobTrackerQueryResponses.JobNotFound(id);
            }

            case SubscribeAck:
            {
                var id = ReadJobId(ref r);
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 2);
                return new JobTrackerQueryResponses.SubscribeAck(id, subscriber);
            }

            case UnsubscribeAck:
            {
                var id = ReadJobId(ref r);
                var subscriber = ReadActorRef(ref r, _system);
                Skip(ref r, fields, 2);
                return new JobTrackerQueryResponses.UnsubscribeAck(id, subscriber);
            }

            case QueueStatus:
            {
                var waiting = r.ReadInt32();
                var running = r.ReadInt32();
                var queued = ReadJobSize(ref r);
                var total = ReadJobSize(ref r);
                var available = ReadJobSize(ref r);

                var count = r.ReadArrayHeader();
                var nodes = ImmutableArray.CreateBuilder<NodeStatus>(count);
                for (var i = 0; i < count; i++)
                {
                    nodes.Add(ReadNodeStatus(ref r));
                }

                // QueuedCount was appended after the first release; a sender on the old schema
                // stops at six fields.
                var queuedCount = 0;
                if (fields > 6)
                    queuedCount = r.ReadInt32();

                Skip(ref r, fields, 7);
                return new JobTrackerQueryResponses.QueueStatus(
                    waiting, running, queued, total, available, nodes.ToImmutable(), queuedCount);
            }

            case JobStatusChanged:
            {
                var id = ReadJobId(ref r);
                var submitter = ReadSubmitterId(ref r);
                var progress = ReadJobProgress(ref r);
                var assigned = ReadAddressOrNull(ref r);

                var submittedAt = progress.LastUpdatedAt;
                if (fields > 4)
                    submittedAt = ReadTimestamp(ref r);

                DateTimeOffset? startedAt = null;
                if (fields > 5)
                    startedAt = ReadTimestampOrNull(ref r);

                Skip(ref r, fields, 6);
                return new JobTrackerNotifications.JobStatusChanged(
                    id, submitter, progress, assigned, submittedAt, startedAt);
            }

            case ExecuteJob:
            {
                var job = ReadJobDefinition(ref r);
                Skip(ref r, fields, 1);
                return new ExecutionMessages.ExecuteJob(job);
            }

            case CancelExecution:
            {
                var id = ReadJobId(ref r);
                var reason = r.ReadString() ?? string.Empty;
                Skip(ref r, fields, 2);
                return new ExecutionMessages.CancelExecution(id, reason);
            }

            default:
                throw new ArgumentException(
                    $"{nameof(JobSchedulingSerializer)} has no reader for manifest '{manifest}'.",
                    nameof(manifest));
        }
    }

    private static JobTrackerState ReadState(ref MessagePackReader r, int fields)
    {
        var pendingCount = r.ReadArrayHeader();
        var pending = ImmutableList.CreateBuilder<JobId>();
        for (var i = 0; i < pendingCount; i++)
        {
            pending.Add(ReadJobId(ref r));
        }

        var jobCount = r.ReadArrayHeader();
        var jobs = ImmutableDictionary.CreateBuilder<JobId, TrackedJob>();
        for (var i = 0; i < jobCount; i++)
        {
            var job = ReadTrackedJob(ref r);
            jobs.Add(job.Id, job);
        }

        var nodeCount = r.ReadArrayHeader();
        var nodes = ImmutableDictionary.CreateBuilder<Address, NodeStatus>();
        for (var i = 0; i < nodeCount; i++)
        {
            var node = ReadNodeStatus(ref r);
            nodes.Add(node.NodeAddress, node);
        }

        Skip(ref r, fields, 3);

        return JobTrackerState.Empty with
        {
            PendingJobs = pending.ToImmutable(),
            Jobs = jobs.ToImmutable(),
            Nodes = nodes.ToImmutable()
        };
    }
}
