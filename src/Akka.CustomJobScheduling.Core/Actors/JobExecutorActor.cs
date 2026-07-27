using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Runs a single job, reporting progress back to the tracker, then stops.
/// </summary>
/// <remarks>
/// One executor per running job, created as a child of <see cref="JobReceiverActor"/>. The "work"
/// is a <c>Task.Delay</c> per tick — the point of this demo is the scheduling, not the computation.
/// </remarks>
public sealed class JobExecutorActor : ReceiveActor
{
    /// <summary>Internal tick: another chunk of work finished.</summary>
    private sealed record WorkCompleted(JobSize Amount);

    /// <summary>Internal: the simulated work threw.</summary>
    private sealed record WorkFailed(string Reason);

    public static Props Props(
        JobDefinition job,
        Address nodeAddress,
        IActorRef tracker,
        IJobExecutionPacer pacer) =>
        Actor.Props.Create(() => new JobExecutorActor(job, nodeAddress, tracker, pacer));

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly JobDefinition _job;
    private readonly Address _nodeAddress;
    private readonly IActorRef _tracker;
    private readonly IJobExecutionPacer _pacer;
    private readonly CancellationTokenSource _shutdown = new();

    private JobSize _completed = JobSize.Zero;

    public JobExecutorActor(
        JobDefinition job,
        Address nodeAddress,
        IActorRef tracker,
        IJobExecutionPacer pacer)
    {
        _job = job;
        _nodeAddress = nodeAddress;
        _tracker = tracker;
        _pacer = pacer;

        Receive<WorkCompleted>(HandleWorkCompleted);
        Receive<WorkFailed>(failed =>
        {
            _log.Warning("Job {JobId} failed: {Reason}", _job.Id.Value, failed.Reason);
            _tracker.Tell(new JobTrackerCommands.ReportJobFailed(_job.Id, _nodeAddress, failed.Reason));
            Context.Stop(Self);
        });
    }

    protected override void PreStart()
    {
        _log.Info("Starting job {JobId} ({JobSize} units)", _job.Id.Value, _job.Size.Size);
        ScheduleNextTick();
    }

    protected override void PostStop()
    {
        // Whatever Task.Delay is in flight outlives the actor otherwise, and would post a
        // WorkCompleted to a dead letter queue on every cancelled job.
        _shutdown.Cancel();
        _shutdown.Dispose();
    }

    private void HandleWorkCompleted(WorkCompleted tick)
    {
        _completed += tick.Amount;

        if (_completed >= _job.Size)
        {
            _log.Info("Completed job {JobId}", _job.Id.Value);
            _tracker.Tell(new JobTrackerCommands.ReportJobCompleted(_job.Id, _nodeAddress));

            // The job is done and this actor has nothing left to do. The receiver is watching and
            // will clean up its bookkeeping when the Terminated arrives.
            Context.Stop(Self);
            return;
        }

        _tracker.Tell(new JobTrackerCommands.ReportProgress(
            _job.Id,
            _nodeAddress,
            new WorkProgress(_completed, _job.Size)));

        ScheduleNextTick();
    }

    private void ScheduleNextTick()
    {
        var delay = _pacer.NextInterval(_job);
        var amount = _pacer.TickSize(_job);
        var token = _shutdown.Token;

        DoWork().PipeTo(Self);
        return;

        async Task<object> DoWork()
        {
            try
            {
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, token);

                return new WorkCompleted(amount);
            }
            catch (OperationCanceledException)
            {
                // Actor is stopping — PipeTo has nowhere useful to deliver, and that's fine.
                return new WorkFailed("Execution cancelled.");
            }
        }
    }
}
