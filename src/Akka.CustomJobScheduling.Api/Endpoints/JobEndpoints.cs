using System.Text.Json;
using Akka.Actor;
using Akka.CustomJobScheduling.Api.Contracts;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Actors.Streaming;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Hosting;

namespace Akka.CustomJobScheduling.Api.Endpoints;

/// <summary>
/// HTTP surface for submitting jobs and watching them run.
/// </summary>
/// <remarks>
/// Nothing here touches <see cref="ActorSystem"/>. Every handler reaches the actor world through
/// <see cref="IRequiredActor{T}"/>, which is what keeps the endpoints testable and keeps actor
/// creation inside a supervised hierarchy.
/// </remarks>
public static class JobEndpoints
{
    private static readonly TimeSpan AskTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Idle gap after which we emit an SSE comment to keep proxies from closing us.</summary>
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(15);

    private static readonly JsonSerializerOptions Json =
        new(JsonSerializerDefaults.Web);

    public static IEndpointRouteBuilder MapJobEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/jobs", SubmitJob);
        app.MapGet("/jobs/{id}", GetJob);
        app.MapDelete("/jobs/{id}", CancelJob);
        app.MapGet("/jobs/{id}/events", StreamJob);
        app.MapGet("/cluster/queue", GetQueue);

        return app;
    }

    private static async Task<IResult> SubmitJob(
        SubmitJobRequest request,
        IRequiredActor<JobSubmitterManagerKey> submitters,
        CancellationToken ct)
    {
        if (request.Size == 0)
            return Results.BadRequest(new { error = "Size must be greater than zero." });

        var id = new JobId(string.IsNullOrWhiteSpace(request.Id)
            ? $"job-{Guid.NewGuid():N}"
            : request.Id);

        var submitter = new JobSubmitterId(string.IsNullOrWhiteSpace(request.SubmitterId)
            ? "anonymous"
            : request.SubmitterId);

        // Routed through the submitter region rather than straight at the tracker, so the submitter
        // entity stays in the loop and remains re-acquirable by id after a recovery. Forward
        // preserves the sender, so the tracker's reply lands back here.
        var response = await submitters.ActorRef.Ask<IJobTrackerCommandResponse>(
            new JobTrackerCommands.SubmitJob(new JobDefinition(id, new JobSize(request.Size)), submitter),
            AskTimeout,
            ct);

        return response switch
        {
            JobTrackerResponses.CommandAccepted =>
                Results.Accepted($"/jobs/{Uri.EscapeDataString(id.Value)}", new { id = id.Value }),

            JobTrackerResponses.CommandRejected rejected => Results.Json(
                new { error = rejected.Message, reason = rejected.Reason.ToString() },
                statusCode: RejectionMapping.ToStatusCode(rejected.Reason)),

            _ => Results.StatusCode(StatusCodes.Status502BadGateway)
        };
    }

    private static async Task<IResult> GetJob(
        string id,
        IRequiredActor<JobTrackerKey> tracker,
        CancellationToken ct)
    {
        var response = await tracker.ActorRef.Ask<IJobTrackerQueryResponse>(
            new JobTrackerQueries.GetJobStatus(new JobId(id)),
            AskTimeout,
            ct);

        return response switch
        {
            JobTrackerQueryResponses.JobStatusResult found =>
                Results.Ok(JobStatusResponse.From(found)),
            _ => Results.NotFound(new { error = $"Job '{id}' is not known to this tracker." })
        };
    }

    /// <summary>
    /// <c>DELETE /jobs/{id}?submitterId=...&amp;reason=...</c>
    /// </summary>
    /// <remarks>
    /// Parameters come from the query string rather than a body: minimal APIs refuse to infer a
    /// body on DELETE, and a body on DELETE is poorly supported by intermediaries anyway.
    /// </remarks>
    private static async Task<IResult> CancelJob(
        string id,
        string submitterId,
        string? reason,
        IRequiredActor<JobSubmitterManagerKey> submitters,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(submitterId))
            return Results.BadRequest(new { error = "submitterId is required." });

        var response = await submitters.ActorRef.Ask<IJobTrackerCommandResponse>(
            new JobTrackerCommands.CancelJob(
                new JobId(id),
                new JobSubmitterId(submitterId),
                reason ?? "Cancelled via API."),
            AskTimeout,
            ct);

        return response switch
        {
            JobTrackerResponses.CommandAccepted => Results.Accepted(),

            JobTrackerResponses.CommandRejected rejected => Results.Json(
                new { error = rejected.Message, reason = rejected.Reason.ToString() },
                statusCode: RejectionMapping.ToStatusCode(rejected.Reason)),

            _ => Results.StatusCode(StatusCodes.Status502BadGateway)
        };
    }

    private static async Task<IResult> GetQueue(
        IRequiredActor<JobTrackerKey> tracker,
        CancellationToken ct)
    {
        var status = await tracker.ActorRef.Ask<JobTrackerQueryResponses.QueueStatus>(
            JobTrackerQueries.GetQueueStatus.Instance,
            AskTimeout,
            ct);

        return Results.Ok(QueueStatusResponse.From(status));
    }

    /// <summary>
    /// Server-sent events for one job, ending when the job reaches a terminal state.
    /// </summary>
    private static async Task<IResult> StreamJob(
        string id,
        HttpContext context,
        IRequiredActor<JobStreamSupervisorKey> streams,
        CancellationToken ct)
    {
        var opened = await streams.ActorRef.Ask<object>(
            new JobStreamMessages.OpenJobStream(new JobId(id)),
            AskTimeout,
            ct);

        if (opened is JobStreamMessages.JobStreamRefused refused)
            return Results.Json(
                new { error = refused.Reason },
                statusCode: StatusCodes.Status503ServiceUnavailable);

        var stream = (JobStreamMessages.JobStreamOpened)opened;

        // Disposing tells the bridge actor to stop, which completes the channel and releases the
        // tracker subscription. Runs on client disconnect, on error, and on normal completion.
        await using (stream)
        {
            context.Response.Headers.ContentType = "text/event-stream";
            context.Response.Headers.CacheControl = "no-cache";
            context.Response.Headers.Connection = "keep-alive";

            // Reverse proxies buffer response bodies by default, which would hold every event until
            // the stream ends and defeat the point.
            context.Response.Headers["X-Accel-Buffering"] = "no";

            await context.Response.Body.FlushAsync(ct);
            await PumpAsync(context, stream, ct);
        }

        return Results.Empty;
    }

    private static async Task PumpAsync(
        HttpContext context,
        JobStreamMessages.JobStreamOpened stream,
        CancellationToken ct)
    {
        try
        {
            while (true)
            {
                // A fresh wait each pass, bounded by the heartbeat interval. Cancelling a
                // WaitToReadAsync never consumes anything, so nothing is dropped when it times out.
                //
                // Deliberately not a PeriodicTimer raced via Task.WhenAny: WaitForNextTickAsync
                // permits only one outstanding call, and the loser of a WhenAny is still pending on
                // the next pass, which throws InvalidOperationException.
                using var idle = CancellationTokenSource.CreateLinkedTokenSource(ct);
                idle.CancelAfter(HeartbeatInterval);

                bool more;
                try
                {
                    more = await stream.Updates.WaitToReadAsync(idle.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // Idle. An SSE comment keeps proxies from closing the connection, and the
                    // client never has to interpret it.
                    await context.Response.WriteAsync(": keep-alive\n\n", ct);
                    await context.Response.Body.FlushAsync(ct);
                    continue;
                }

                if (!more)
                    return; // channel completed - job reached a terminal state

                while (stream.Updates.TryRead(out var status))
                {
                    var payload = JsonSerializer.Serialize(JobStatusResponse.From(status), Json);
                    await context.Response.WriteAsync($"event: status\ndata: {payload}\n\n", ct);
                }

                await context.Response.Body.FlushAsync(ct);
            }
        }
        catch (OperationCanceledException)
        {
            // Client hung up. Normal for SSE; the `await using` still releases the subscription.
        }
    }
}
