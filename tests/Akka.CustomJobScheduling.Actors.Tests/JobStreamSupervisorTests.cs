using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Actors.Streaming;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Hosting;
using Xunit.Abstractions;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// The bridge between a tracker subscription and something an HTTP handler can read.
/// </summary>
/// <remarks>
/// These live in Core rather than the API project precisely so they can be tested here, as actors,
/// without spinning up a web host. The HTTP framing is a separate concern.
/// </remarks>
public class JobStreamSupervisorTests : JobSchedulingTestKit
{
    public JobStreamSupervisorTests(ITestOutputHelper output) : base(output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithJobStreams();
    }

    private IActorRef Streams => ActorRegistry.Get<JobStreamSupervisorKey>();

    /// <summary>Bounds every channel read so a broken stream fails the test instead of hanging.</summary>
    private CancellationToken Deadline => new CancellationTokenSource(RemainingOrDefault).Token;

    private Task<object> OpenAsync(string jobId) =>
        Streams.Ask<object>(
            new JobStreamMessages.OpenJobStream(new JobId(jobId)),
            RemainingOrDefault);

    [Fact]
    public async Task Opening_a_stream_yields_the_job_current_status_immediately()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 40);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        var stream = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-1"));
        await using var _ = stream;

        var first = await stream.Updates.ReadAsync(Deadline);
        Assert.Equal(new JobId("job-1"), first.Id);
        Assert.Equal(JobStatus.Running, first.Progress.Status);
    }

    [Fact]
    public async Task The_channel_completes_on_its_own_when_the_job_finishes()
    {
        Membership.MemberUp("node-a", 100);
        Pacer.RunToCompletion(unitsPerTick: 1);
        await SubmitAsync("job-1", 4);

        var stream = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-1"));
        await using var _ = stream;

        JobTrackerNotifications.JobStatusChanged? last = null;

        // A reader can simply drain to completion - it never has to inspect the payload to know
        // when to stop, which is what makes the SSE loop in the API a plain await foreach.
        await foreach (var update in stream.Updates.ReadAllAsync(Deadline))
        {
            last = update;
        }

        Assert.NotNull(last);
        Assert.Equal(JobStatus.Completed, last.Progress.Status);
    }

    [Fact]
    public async Task Two_clients_can_watch_the_same_job()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 40);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        var first = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-1"));
        var second = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-1"));
        await using var _ = first;
        await using var __ = second;

        Assert.NotEqual(first.Stream, second.Stream);

        var ct = Deadline;
        Assert.Equal(new JobId("job-1"), (await first.Updates.ReadAsync(ct)).Id);
        Assert.Equal(new JobId("job-1"), (await second.Updates.ReadAsync(ct)).Id);
    }

    [Fact]
    public async Task Disposing_a_stream_stops_its_actor_and_drops_the_subscription()
    {
        Membership.MemberUp("node-a", 100);
        await SubmitAsync("job-1", 40);
        await AwaitStatusAsync("job-1", JobStatus.Running);

        var stream = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-1"));
        await WatchAsync(stream.Stream);

        await stream.DisposeAsync();

        await ExpectTerminatedAsync(stream.Stream);

        // PostStop completes the channel, so a reader drains whatever was buffered and then exits
        // instead of hanging until the job happens to finish. That is precisely what ends the SSE
        // handler's `await foreach` when a client disconnects mid-job. If it were still open this
        // enumeration would block until Deadline and fail the test.
        await foreach (var _ in stream.Updates.ReadAllAsync(Deadline))
        {
        }

        Assert.True(stream.Updates.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task A_stream_for_an_unknown_job_stays_open_until_that_job_appears()
    {
        Membership.MemberUp("node-a", 100);

        // Subscribing before submitting is legal - the tracker holds the subscription and the
        // client sees the job from its very first transition.
        var stream = Assert.IsType<JobStreamMessages.JobStreamOpened>(await OpenAsync("job-later"));
        await using var _ = stream;

        await SubmitAsync("job-later", 20);

        var first = await stream.Updates.ReadAsync(Deadline);
        Assert.Equal(new JobId("job-later"), first.Id);
    }
}
