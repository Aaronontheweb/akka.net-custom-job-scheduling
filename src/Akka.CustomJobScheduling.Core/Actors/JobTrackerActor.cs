using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;
using Akka.Hosting;

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
public sealed class JobTrackerActor : ReceiveActor, IWithTimers
{
    /// <summary>Well-known name, also the singleton name in clustered mode.</summary>
    public const string Name = "job-tracker";

    private const string DrainTimerKey = "drain-queue";
    private static readonly TimeSpan DrainInterval = TimeSpan.FromSeconds(5);

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IClusterMembershipSource _membership;
    private readonly IJobReceiverRouter _receivers;
    private readonly TimeProvider _time;
    private readonly IRequiredActor<JobSubmitterManagerKey> _submitters;

    private readonly Dictionary<JobId, HashSet<IActorRef>> _subscribers = [];

    private JobTrackerState _state = JobTrackerState.Empty;

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

        Receive<IJobTrackerCommand>(HandleCommand);
        Receive<JobTrackerQueries.GetJobStatus>(query => Sender.Tell(_state.GetJobStatus(query.Id)));
        Receive<JobTrackerQueries.GetQueueStatus>(_ => Sender.Tell(_state.GetQueueStatus()));
        Receive<JobTrackerQueries.SubscribeToJob>(HandleSubscribe);
        Receive<JobTrackerQueries.UnsubscribeFromJob>(HandleUnsubscribe);
        Receive<Terminated>(terminated => DropSubscriber(terminated.ActorRef));
    }

    protected override void PreStart()
    {
        _membership.Subscribe(Self);
        Timers.StartPeriodicTimer(
            DrainTimerKey,
            JobTrackerCommands.DrainQueue.Instance,
            DrainInterval,
            DrainInterval);
    }

    protected override void PostStop() => _membership.Unsubscribe(Self);

    private void HandleCommand(IJobTrackerCommand command)
    {
        var decision = _state.Decide(command, _time.GetUtcNow());

        // Persistence slots in exactly here: PersistAll(decision.Events, ...) and move the rest of
        // this method into the callback. Nothing else about the actor has to change.
        _state = _state.Fold(decision.Events);

        if (decision.Response is not null)
            Sender.Tell(decision.Response);

        foreach (var @event in decision.Events)
        {
            React(@event);
        }
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

    private void NotifyAbout(JobId id)
    {
        if (!_state.Jobs.TryGetValue(id, out var job))
            return;

        var notification = job.ToNotification();

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

    private void DropSubscriber(IActorRef subscriber)
    {
        foreach (var id in _subscribers.Keys.ToList())
        {
            var listeners = _subscribers[id];
            listeners.Remove(subscriber);

            if (listeners.Count == 0)
                _subscribers.Remove(id);
        }
    }
}
