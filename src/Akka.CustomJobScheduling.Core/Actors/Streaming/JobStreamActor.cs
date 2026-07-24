using System.Threading.Channels;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors.Streaming;

/// <summary>
/// Bridges one job's tracker subscription into a <see cref="Channel{T}"/> an HTTP handler can read.
/// </summary>
/// <remarks>
/// An HTTP response isn't an <see cref="IActorRef"/>, so something has to hold the subscription on
/// its behalf and live exactly as long as the connection does. That's this actor: it subscribes on
/// start, writes every push into the channel, and completes the channel when the job finishes so
/// the reader's loop ends on its own.
/// </remarks>
internal sealed class JobStreamActor : ReceiveActor
{
    public static Props Props(
        JobId id,
        IActorRef tracker,
        ChannelWriter<JobTrackerNotifications.JobStatusChanged> updates) =>
        Actor.Props.Create(() => new JobStreamActor(id, tracker, updates));

    private readonly JobId _id;
    private readonly IActorRef _tracker;
    private readonly ChannelWriter<JobTrackerNotifications.JobStatusChanged> _updates;

    public JobStreamActor(
        JobId id,
        IActorRef tracker,
        ChannelWriter<JobTrackerNotifications.JobStatusChanged> updates)
    {
        _id = id;
        _tracker = tracker;
        _updates = updates;

        Receive<JobTrackerNotifications.JobStatusChanged>(status =>
        {
            // The channel is bounded and drops oldest, which is safe only because every
            // notification carries absolute progress rather than a delta: a reader that misses
            // 40% -> 50% is still correct once it sees 60%.
            _updates.TryWrite(status);

            if (status.Progress.IsTerminal)
                Context.Stop(Self);
        });

        // The tracker acks the subscription; nothing to do but not leave it unhandled.
        Receive<JobTrackerQueryResponses.SubscribeAck>(_ => { });
    }

    protected override void PreStart() =>
        _tracker.Tell(new JobTrackerQueries.SubscribeToJob(_id, Self));

    protected override void PostStop() => _updates.TryComplete();
}
