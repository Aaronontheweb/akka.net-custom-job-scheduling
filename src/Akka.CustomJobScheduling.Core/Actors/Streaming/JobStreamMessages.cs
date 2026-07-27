using System.Threading.Channels;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors.Streaming;

/// <summary>
/// Protocol for opening a live status feed for a single job.
/// </summary>
/// <remarks>
/// <b>These messages are strictly in-process.</b> <see cref="JobStreamOpened"/> carries a
/// <see cref="ChannelReader{T}"/> and a reference to a child actor; neither is serializable, and
/// neither would mean anything on another node. The supervisor is node-local by design — do not
/// route this protocol through a shard region or a singleton proxy.
/// </remarks>
public static class JobStreamMessages
{
    /// <summary>Request a live feed of status changes for <paramref name="Id"/>.</summary>
    public sealed record OpenJobStream(JobId Id) : IWithJobId;

    /// <summary>
    /// Request a live feed of the whole queue: an opening snapshot, then every job transition and
    /// capacity change.
    /// </summary>
    public sealed record OpenQueueStream;

    /// <summary>
    /// A feed. Read <see cref="Updates"/> until it completes, then dispose to release the actor.
    /// </summary>
    /// <param name="Stream">The backing actor; disposing tells it to stop.</param>
    /// <param name="Updates">
    /// Status changes as they happen. Completes on its own when the job reaches a terminal state,
    /// so a well-behaved reader ends without needing to inspect the payload.
    /// </param>
    public sealed record JobStreamOpened(
        IActorRef Stream,
        ChannelReader<JobTrackerNotifications.JobStatusChanged> Updates) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            Stream.Tell(PoisonPill.Instance);
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// A whole-queue feed. Carries a mix of <see cref="JobTrackerQueryResponses.JobList"/>,
    /// <see cref="JobTrackerQueryResponses.QueueStatus"/> and
    /// <see cref="JobTrackerNotifications.JobStatusChanged"/>, so the reader pattern-matches.
    /// </summary>
    /// <remarks>
    /// Unlike a job feed this never completes on its own — there is no terminal state for "the
    /// queue". It ends when the reader disposes.
    /// </remarks>
    public sealed record QueueStreamOpened(IActorRef Stream, ChannelReader<object> Updates)
        : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            Stream.Tell(PoisonPill.Instance);
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>The node is already carrying as many feeds as it will allow.</summary>
    public sealed record JobStreamRefused(JobId Id, string Reason) : IWithJobId;
}
