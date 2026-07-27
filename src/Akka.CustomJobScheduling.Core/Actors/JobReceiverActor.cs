using System.Web;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// This node's entry point for work. One per node, top-level, spawning a
/// <see cref="JobExecutorActor"/> child per job it is given.
/// </summary>
/// <remarks>
/// The receiver knows two things the executor shouldn't have to: which node this is, and how to
/// reach the tracker. It passes both down so the executor can report progress directly rather than
/// relaying every tick through here.
/// </remarks>
public sealed class JobReceiverActor : ReceiveActor
{
    /// <summary>Well-known name — <see cref="Cluster.IJobReceiverRouter"/> resolves against it.</summary>
    public const string Name = "job-receiver";

    public static Props Props(Address nodeAddress, IActorRef tracker, IJobExecutionPacer pacer) =>
        Actor.Props.Create(() => new JobReceiverActor(nodeAddress, tracker, pacer));

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly Address _nodeAddress;
    private readonly IActorRef _tracker;
    private readonly IJobExecutionPacer _pacer;
    private readonly Dictionary<JobId, IActorRef> _running = [];

    public JobReceiverActor(Address nodeAddress, IActorRef tracker, IJobExecutionPacer pacer)
    {
        _nodeAddress = nodeAddress;
        _tracker = tracker;
        _pacer = pacer;

        Receive<ExecutionMessages.ExecuteJob>(HandleExecute);
        Receive<ExecutionMessages.CancelExecution>(HandleCancel);
        Receive<Terminated>(HandleTerminated);
    }

    private void HandleExecute(ExecutionMessages.ExecuteJob execute)
    {
        // The tracker can legitimately redeliver — a requeue that lands back on this node, or a
        // retry after a dropped message. Running it twice would double-count capacity.
        if (_running.ContainsKey(execute.Id))
        {
            _log.Debug("Job {JobId} is already running here; ignoring duplicate dispatch",
                execute.Id.Value);
            return;
        }

        var executor = Context.ActorOf(
            JobExecutorActor.Props(execute.Job, _nodeAddress, _tracker, _pacer),
            ChildName(execute.Id));

        Context.Watch(executor);
        _running[execute.Id] = executor;
    }

    private void HandleCancel(ExecutionMessages.CancelExecution cancel)
    {
        if (!_running.TryGetValue(cancel.Id, out var executor))
            return;

        _log.Info("Cancelling job {JobId}: {Reason}", cancel.Id.Value, cancel.Reason);

        // Stopping the child is enough — the tracker already recorded the cancellation, so the
        // executor must not report anything further about this job.
        _running.Remove(cancel.Id);
        Context.Stop(executor);
    }

    private void HandleTerminated(Terminated terminated)
    {
        var finished = _running
            .Where(entry => entry.Value.Equals(terminated.ActorRef))
            .Select(entry => entry.Key)
            .ToList();

        foreach (var id in finished)
        {
            _running.Remove(id);
        }
    }

    /// <summary>
    /// Actor names must be URI-friendly, and a <see cref="JobId"/> is an arbitrary string.
    /// </summary>
    private static string ChildName(JobId id) => HttpUtility.UrlEncode(id.Value);
}
