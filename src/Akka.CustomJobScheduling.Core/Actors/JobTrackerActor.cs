using System.Collections.Immutable;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Owns <see cref="JobTrackerState"/> and drives it.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately thin. Every scheduling rule — validation, placement, capacity accounting, requeue
/// on node loss — lives in <see cref="JobTrackerState"/> and is already tested without an
/// <c>ActorSystem</c>. What is left here is only the part that genuinely needs an actor:
/// </para>
/// <list type="number">
/// <item><description>read the clock</description></item>
/// <item><description>hand the command to <see cref="JobTrackerState.Decide"/></description></item>
/// <item><description>fold the resulting events back in (later: persist them first)</description></item>
/// <item><description>reply to <c>Sender</c></description></item>
/// <item><description>act on the events — dispatch work, notify subscribers</description></item>
/// </list>
/// <para>
/// Subscriptions live here rather than in <see cref="JobTrackerState"/> on purpose: an
/// <see cref="IActorRef"/> is meaningless after a restart, so persisting one would be a lie.
/// </para>
/// </remarks>
public sealed class JobTrackerActor : ReceivePersistentActor, IWithTimers
{
    /// <summary>Well-known name, also the singleton name in clustered mode.</summary>
    public const string Name = "job-tracker";

    /// <summary>
    /// Fixed, because there is exactly one tracker in the cluster. A singleton that failed over to
    /// another node must resume the same stream, so this cannot include the node identity.
    /// </summary>
    public const string PersistentId = "job-tracker";

    /// <summary>
    /// Snapshot cadence. Every this many events the tracker snapshots and then trims the journal up to
    /// that point, so replay time and journal size both stay bounded instead of growing for the life of
    /// the singleton.
    /// </summary>
    private const int SnapshotEvery = 100;

    private const string DrainTimerKey = "drain-queue";
    private static readonly TimeSpan DrainInterval = TimeSpan.FromSeconds(5);

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IClusterMembershipSource _membership;
    private readonly IJobReceiverRouter _receivers;
    private readonly TimeProvider _time;
    private readonly IRequiredActor<JobSubmitterManagerKey> _submitters;

    private readonly Dictionary<JobId, HashSet<IActorRef>> _subscribers = [];

    /// <summary>Watchers of the whole queue rather than one job — dashboards, essentially.</summary>
    private readonly HashSet<IActorRef> _queueSubscribers = [];

    private JobTrackerState _state = JobTrackerState.Empty;

    /// <summary>
    /// Sequence number of the most recent snapshot. Cadence is measured from here rather than from an
    /// exact multiple of <see cref="SnapshotEvery"/>, which a multi-event batch can step straight over.
    /// </summary>
    private long _lastSnapshotSequenceNr;

    public ITimerScheduler Timers { get; set; } = null!;

    public JobTrackerActor(
        IClusterMembershipSource membership,
        IJobReceiverRouter receivers,
        IRequiredActor<JobSubmitterManagerKey> submitters,
        TimeProvider time)
    {
        _membership = membership;
        _receivers = receivers;
        _submitters = submitters;
        _time = time;

        Command<IJobTrackerCommand>(HandleCommand);
        Command<IJobTrackerFact>(HandleFact);
        Command<JobTrackerQueries.GetJobStatus>(query => Sender.Tell(_state.GetJobStatus(query.Id)));
        Command<JobTrackerQueries.GetQueueStatus>(_ => Sender.Tell(_state.GetQueueStatus()));
        Command<JobTrackerQueries.GetJobs>(query =>
            Sender.Tell(_state.GetJobs(query.IncludeFinished, query.Limit)));
        Command<JobTrackerQueries.SubscribeToJob>(HandleSubscribe);
        Command<JobTrackerQueries.UnsubscribeFromJob>(HandleUnsubscribe);
        Command<JobTrackerQueries.SubscribeToQueue>(HandleSubscribeToQueue);
        Command<JobTrackerQueries.UnsubscribeFromQueue>(HandleUnsubscribeFromQueue);
        Command<Terminated>(terminated => DropSubscriber(terminated.ActorRef));
        Command<SaveSnapshotSuccess>(success =>
        {
            // The snapshot captures state through its sequence number, so every journal entry up to and
            // including it is now redundant: recovery loads the snapshot and replays only what follows.
            // Trim the journal and the older snapshots so neither grows for the life of the singleton.
            DeleteMessages(success.Metadata.SequenceNr);
            DeleteSnapshots(new SnapshotSelectionCriteria(success.Metadata.SequenceNr - 1));
        });
        Command<SaveSnapshotFailure>(failure =>
            _log.Warning("Snapshot at sequence {SequenceNr} failed: {Reason}",
                failure.Metadata.SequenceNr, failure.Cause.Message));
        Command<DeleteSnapshotsSuccess>(_ => { });
        Command<DeleteSnapshotsFailure>(_ => { });
        Command<DeleteMessagesSuccess>(_ => { });
        Command<DeleteMessagesFailure>(failure =>
            _log.Warning("Trimming the journal to sequence {SequenceNr} failed: {Reason}",
                failure.ToSequenceNr, failure.Cause.Message));

        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is not JobTrackerState snapshot)
                return;

            _state = snapshot;
            _lastSnapshotSequenceNr = offer.Metadata.SequenceNr;
        });

        Recover<IJobTrackerEvent>(@event => _state = _state.Apply(@event));
        Recover<RecoveryCompleted>(_ => OnRecoveryCompleted());
    }

    public override string PersistenceId => PersistentId;

    private void OnRecoveryCompleted()
    {
        _log.Info(
            "Recovered tracker at sequence {SequenceNr}: {JobCount} jobs, {NodeCount} nodes",
            LastSequenceNr,
            _state.Jobs.Count,
            _state.Nodes.Count);

        // Subscribe only once recovery is done. The membership source replies with the full member
        // set, and reconciling that against the restored node map is what drops nodes that left
        // while this tracker (or its predecessor on another host) was down.
        _membership.Subscribe(Self);

        Timers.StartPeriodicTimer(
            DrainTimerKey,
            JobTrackerFacts.DrainQueue.Instance,
            DrainInterval,
            DrainInterval);
    }

    protected override void PostStop() => _membership.Unsubscribe(Self);

    /// <summary>
    /// A request that can be answered yes or no. <see cref="JobTrackerState.Decide"/> validates it;
    /// a rejection is a reply the sender is waiting on.
    /// </summary>
    private void HandleCommand(IJobTrackerCommand command)
    {
        var decision = _state.Decide(command, _time.GetUtcNow());

        if (decision.Events.IsEmpty)
        {
            if (decision.Response is not null)
                Sender.Tell(decision.Response);

            return;
        }

        // Sender is captured because the persist callback runs later, by which point Sender belongs
        // to whatever message is being handled then.
        var replyTo = Sender;

        PersistAndApply(decision.Events, () =>
        {
            if (decision.Response is not null)
                replyTo.Tell(decision.Response);
        });
    }

    /// <summary>
    /// Something that already happened — a worker report, a membership change.
    /// <see cref="JobTrackerState.Integrate"/> folds it into events; there is nothing to reply,
    /// because a fact can't be refused.
    /// </summary>
    private void HandleFact(IJobTrackerFact fact)
    {
        var events = _state.Integrate(fact, _time.GetUtcNow());

        if (!events.IsEmpty)
            PersistAndApply(events, onComplete: null);
    }

    /// <summary>
    /// Persists events, then folds and reacts to each once it's durable. Shared by commands and
    /// facts — the only difference between them is what runs after the last event lands.
    /// </summary>
    /// <remarks>
    /// React runs only after the event is durable. Dispatching work we haven't recorded would leave
    /// a job running on a node the tracker forgets about the moment it restarts.
    /// </remarks>
    private void PersistAndApply(ImmutableArray<IJobTrackerEvent> events, Action? onComplete)
    {
        var remaining = events.Length;

        PersistAll(events, @event =>
        {
            _state = _state.Apply(@event);
            React(@event);

            if (--remaining > 0)
                return;

            onComplete?.Invoke();

            // Measure cadence from the last snapshot's sequence, not against an exact multiple: a
            // multi-event batch (submit-with-placement, a node-loss fan-out) advances LastSequenceNr in
            // one step and would otherwise leap straight over the boundary and skip the snapshot.
            if (LastSequenceNr - _lastSnapshotSequenceNr >= SnapshotEvery)
            {
                SaveSnapshot(_state);
                _lastSnapshotSequenceNr = LastSequenceNr;
            }
        });
    }

    /// <summary>
    /// Turns a recorded fact into outbound side effects. The state has already been updated by the
    /// time this runs, so lookups here see the post-event world.
    /// </summary>
    private void React(IJobTrackerEvent @event)
    {
        switch (@event)
        {
            case JobTrackerEvents.JobScheduled scheduled:
                Dispatch(scheduled);
                break;

            case JobTrackerEvents.JobRequeued requeued:
                // Best-effort: the node is usually already gone, but if it merely lost a job we
                // don't want a duplicate execution when it comes back.
                _receivers.Select(requeued.PreviousNode)
                    .Tell(new ExecutionMessages.CancelExecution(requeued.Id, requeued.Reason));
                break;

            case JobTrackerEvents.JobCancelled cancelled:
                CancelOnAssignedNode(cancelled.Id, cancelled.Reason);
                break;
        }

        if (@event is IWithJobId withJobId)
            NotifyAbout(withJobId.Id);

        // Capacity moves on job transitions as well as topology changes, so refresh watchers after
        // every event rather than trying to guess which ones matter.
        if (_queueSubscribers.Count > 0)
            NotifyQueueWatchers(_state.GetQueueStatus());
    }

    private void Dispatch(JobTrackerEvents.JobScheduled scheduled)
    {
        if (!_state.Jobs.TryGetValue(scheduled.Id, out var job))
            return;

        _log.Info(
            "Dispatching job {JobId} ({JobSize} units) to {NodeAddress}",
            scheduled.Id.Value,
            job.Size.Size,
            scheduled.NodeAddress);

        _receivers.Select(scheduled.NodeAddress)
            .Tell(new ExecutionMessages.ExecuteJob(job.Definition), Self);
    }

    private void CancelOnAssignedNode(JobId id, string reason)
    {
        // The terminal event already cleared AssignedNode, so we can't read it off the job any
        // more. Cancellation is idempotent, so telling every known node is harmless and avoids
        // threading the pre-event node through the event itself.
        foreach (var node in _state.Nodes.Keys)
        {
            _receivers.Select(node).Tell(new ExecutionMessages.CancelExecution(id, reason));
        }
    }

    /// <summary>
    /// Pushes fresh cluster capacity to queue watchers. Job transitions move capacity too, so this
    /// runs for those as well as for topology changes.
    /// </summary>
    private void NotifyQueueWatchers(object update)
    {
        foreach (var watcher in _queueSubscribers)
        {
            watcher.Tell(update);
        }
    }

    private void NotifyAbout(JobId id)
    {
        if (!_state.Jobs.TryGetValue(id, out var job))
            return;

        var notification = job.ToNotification();

        // Queue watchers see every job, including ones they've never heard of — that's the whole
        // point, since a dashboard has no ids in hand when it connects.
        NotifyQueueWatchers(notification);

        // Back to the submitter, routed by JobSubmitterId through the shard region so it survives
        // the submitter having been moved or restarted since it asked. Resolved lazily rather than
        // in the constructor to keep tracker and submitter registration order independent.
        _submitters.ActorRef.Tell(notification);

        if (!_subscribers.TryGetValue(id, out var listeners))
            return;

        foreach (var listener in listeners)
        {
            listener.Tell(notification);
        }

        if (job.IsTerminal)
            _subscribers.Remove(id);
    }

    private void HandleSubscribe(JobTrackerQueries.SubscribeToJob query)
    {
        if (!_subscribers.TryGetValue(query.Id, out var listeners))
        {
            listeners = [];
            _subscribers[query.Id] = listeners;
        }

        if (listeners.Add(query.Subscriber))
            Context.Watch(query.Subscriber);

        Sender.Tell(new JobTrackerQueryResponses.SubscribeAck(query.Id, query.Subscriber));

        // Send current status immediately so a subscriber never has to poll for the state it
        // missed by arriving late.
        if (_state.Jobs.TryGetValue(query.Id, out var job))
            query.Subscriber.Tell(job.ToNotification());
    }

    private void HandleUnsubscribe(JobTrackerQueries.UnsubscribeFromJob query)
    {
        if (_subscribers.TryGetValue(query.Id, out var listeners))
        {
            listeners.Remove(query.Subscriber);

            if (listeners.Count == 0)
                _subscribers.Remove(query.Id);
        }

        Sender.Tell(new JobTrackerQueryResponses.UnsubscribeAck(query.Id, query.Subscriber));
    }

    private void HandleSubscribeToQueue(JobTrackerQueries.SubscribeToQueue query)
    {
        if (_queueSubscribers.Add(query.Subscriber))
            Context.Watch(query.Subscriber);

        // Opening snapshot, so a dashboard renders immediately instead of staying blank until the
        // next thing happens to change.
        query.Subscriber.Tell(_state.GetJobs());
        query.Subscriber.Tell(_state.GetQueueStatus());
    }

    private void HandleUnsubscribeFromQueue(JobTrackerQueries.UnsubscribeFromQueue query) =>
        _queueSubscribers.Remove(query.Subscriber);

    private void DropSubscriber(IActorRef subscriber)
    {
        _queueSubscribers.Remove(subscriber);

        foreach (var id in _subscribers.Keys.ToList())
        {
            var listeners = _subscribers[id];
            listeners.Remove(subscriber);

            if (listeners.Count == 0)
                _subscribers.Remove(id);
        }
    }
}
