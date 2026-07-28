using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Stands in for whoever wanted a job done. One entity per <see cref="JobSubmitterId"/>.
/// </summary>
/// <remarks>
/// In a real system the traffic behind this would come from a web API. Here it just forwards
/// submissions to the tracker and logs the progress the tracker pushes back — which is the point:
/// it proves the notification round-trip works, including re-acquiring a submitter by id rather
/// than by a stale <see cref="IActorRef"/>.
/// </remarks>
public sealed class JobSubmitterActor : ReceiveActor
{
    public static Props Props(JobSubmitterId submitterId, IActorRef tracker) =>
        Actor.Props.Create(() => new JobSubmitterActor(submitterId, tracker));

    private readonly ILoggingAdapter _log = Context.GetLogger();

    public JobSubmitterActor(JobSubmitterId submitterId, IActorRef tracker)
    {
        Receive<JobTrackerCommands.SubmitJob>(submit =>
        {
            _log.Info("Submitter {SubmitterId} requesting job {JobId} ({JobSize} units)",
                submitterId.Value,
                submit.Id.Value,
                submit.Job.Size.Size);

            tracker.Forward(submit);
        });

        Receive<JobTrackerCommands.CancelJob>(cancel => tracker.Forward(cancel));

        Receive<JobTrackerNotifications.JobStatusChanged>(status =>
            _log.Info(
                "Submitter {SubmitterId} job {JobId} is {JobStatus} ({PercentComplete:P0}) on {NodeAddress}",
                submitterId.Value,
                status.Id.Value,
                status.Progress.Status,
                status.Progress.Progress.Fraction,
                status.AssignedNode?.ToString() ?? "no node"));

        Receive<JobTrackerResponses.CommandRejected>(rejected =>
            _log.Warning("Submitter {SubmitterId} job {JobId} rejected: {Reason}",
                submitterId.Value,
                rejected.Id.Value,
                rejected.Message));

        Receive<JobTrackerResponses.CommandAccepted>(_ => { });
    }
}

/// <summary>
/// Routes anything carrying a <see cref="JobSubmitterId"/> to that submitter's entity.
/// </summary>
/// <remarks>
/// Keys off the <see cref="IWithJobSubmitterId"/> interface that already exists on the domain
/// messages, so the same extractor drives both a real shard region and
/// <see cref="GenericChildPerEntityParent"/>.
/// </remarks>
public sealed class JobSubmitterMessageExtractor : HashCodeMessageExtractor
{
    public const int DefaultShardCount = 40;

    public JobSubmitterMessageExtractor(int maxNumberOfShards = DefaultShardCount)
        : base(maxNumberOfShards)
    {
    }

    /// <remarks>
    /// The entity id doubles as the entity's actor name - in a shard region and in
    /// <see cref="GenericChildPerEntityParent"/> alike - so it has to be URI-safe. A submitter id is
    /// user input and generally isn't, so it's URI-encoded here rather than rejected at the edge. The
    /// entity decodes it back to the real id (see the submitter props factory). Encoding is stable, so
    /// a submission and the notification routed back to it land on the same entity.
    /// </remarks>
    public override string? EntityId(object message) => message switch
    {
        IWithJobSubmitterId withSubmitter => Uri.EscapeDataString(withSubmitter.SubmitterId.Value),
        _ => null
    };
}
