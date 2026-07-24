using System.Threading.Channels;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;
using Akka.Hosting;

namespace Akka.CustomJobScheduling.Core.Actors.Streaming;

/// <summary>
/// Owns every open job feed on this node.
/// </summary>
/// <remarks>
/// <para>
/// The alternative — having the HTTP handler call <c>system.ActorOf</c> per connection — puts each
/// bridge directly under the <c>/user</c> guardian. A bridge that throws then escalates to the root
/// guardian, and one flaky connection can take down the <see cref="ActorSystem"/>. Under a real
/// parent it just stops.
/// </para>
/// <para>
/// The parent also gives us a place to cap concurrency, a count worth exporting as a metric, and
/// readable actor paths — <c>job-42-a1b2c3</c> rather than <c>$a</c>.
/// </para>
/// </remarks>
public sealed class JobStreamSupervisor : ReceiveActor
{
    /// <summary>Well-known name.</summary>
    public const string Name = "job-streams";

    /// <summary>Refuse beyond this many concurrent feeds on a single node.</summary>
    public const int MaxConcurrentStreams = 500;

    /// <summary>
    /// Buffered updates per feed. Small on purpose: a slow client should shed intermediate
    /// progress, not accumulate it.
    /// </summary>
    private const int UpdateBufferSize = 32;

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _tracker;
    private readonly Dictionary<IActorRef, JobId> _open = [];

    public JobStreamSupervisor(IRequiredActor<JobTrackerKey> tracker)
    {
        _tracker = tracker.ActorRef;

        Receive<JobStreamMessages.OpenJobStream>(Open);
        Receive<Terminated>(Closed);
        Receive<JobTrackerQueryResponses.UnsubscribeAck>(_ => { });
    }

    private void Open(JobStreamMessages.OpenJobStream request)
    {
        if (_open.Count >= MaxConcurrentStreams)
        {
            _log.Warning("Refusing job feed for {JobId}: {OpenStreams} already open",
                request.Id.Value, _open.Count);

            Sender.Tell(new JobStreamMessages.JobStreamRefused(
                request.Id,
                $"This node is already serving {MaxConcurrentStreams} job feeds."));
            return;
        }

        var updates = Channel.CreateBounded<JobTrackerNotifications.JobStatusChanged>(
            new BoundedChannelOptions(UpdateBufferSize)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = true
            });

        // Named per connection rather than per job — several clients may watch the same job.
        var child = Context.ActorOf(
            JobStreamActor.Props(request.Id, _tracker, updates.Writer),
            $"{Uri.EscapeDataString(request.Id.Value)}-{Guid.NewGuid():N}");

        Context.Watch(child);
        _open[child] = request.Id;

        Sender.Tell(new JobStreamMessages.JobStreamOpened(child, updates.Reader));
    }

    private void Closed(Terminated terminated)
    {
        if (!_open.Remove(terminated.ActorRef, out var id))
            return;

        // The tracker watches its subscribers too, so this is belt-and-braces — but it releases the
        // slot immediately instead of waiting on the failure detector, and the ack comes back here
        // rather than to a dead actor.
        _tracker.Tell(new JobTrackerQueries.UnsubscribeFromJob(id, terminated.ActorRef));
    }

    /// <summary>
    /// A bridge whose channel and HTTP response are already gone has nothing to recover into, and
    /// restarting it would re-subscribe to the tracker and leak. Stop it instead.
    /// </summary>
    protected override SupervisorStrategy SupervisorStrategy() =>
        new OneForOneStrategy(_ => Directive.Stop);
}
