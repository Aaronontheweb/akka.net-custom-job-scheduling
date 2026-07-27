using System.Threading.Channels;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;

namespace Akka.CustomJobScheduling.Core.Actors.Streaming;

/// <summary>
/// Bridges a whole-queue subscription into a channel an HTTP handler can read.
/// </summary>
/// <remarks>
/// The per-job sibling of this actor stops once its job reaches a terminal state. A queue feed has
/// no such end — it runs until the reader goes away, which for SSE means until the browser tab
/// closes.
/// </remarks>
internal sealed class QueueStreamActor : ReceiveActor
{
    public static Props Props(IActorRef tracker, ChannelWriter<object> updates) =>
        Actor.Props.Create(() => new QueueStreamActor(tracker, updates));

    private readonly IActorRef _tracker;
    private readonly ChannelWriter<object> _updates;

    public QueueStreamActor(IActorRef tracker, ChannelWriter<object> updates)
    {
        _tracker = tracker;
        _updates = updates;

        Receive<JobTrackerQueryResponses.JobList>(list => _updates.TryWrite(list));
        Receive<JobTrackerQueryResponses.QueueStatus>(status => _updates.TryWrite(status));
        Receive<JobTrackerNotifications.JobStatusChanged>(status => _updates.TryWrite(status));
    }

    protected override void PreStart() =>
        _tracker.Tell(new JobTrackerQueries.SubscribeToQueue(Self));

    protected override void PostStop()
    {
        _tracker.Tell(new JobTrackerQueries.UnsubscribeFromQueue(Self));
        _updates.TryComplete();
    }
}
