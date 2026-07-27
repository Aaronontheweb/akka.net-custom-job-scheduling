using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Api.Contracts;

/*
 * Wire contracts, kept separate from the domain records on purpose.
 *
 * JobStatusResult and JobStatusChanged carry Akka.Actor.Address and Akka.Cluster.MemberStatus,
 * which System.Text.Json has no business trying to serialize. Mapping through these types also
 * means the HTTP surface doesn't shift every time an internal record gains a field.
 */

/// <summary>Request body for <c>POST /jobs</c>.</summary>
/// <param name="Id">Caller-supplied job id. Optional — one is generated when omitted.</param>
/// <param name="Size">Work units. Must be greater than zero.</param>
/// <param name="SubmitterId">Who is asking. Optional; defaults to an anonymous submitter.</param>
public sealed record SubmitJobRequest(string? Id, uint Size, string? SubmitterId);

/// <summary>A job's current state.</summary>
public sealed record JobStatusResponse(
    string Id,
    string SubmitterId,
    string Status,
    uint Completed,
    uint Total,
    decimal Fraction,
    string? AssignedNode,
    DateTimeOffset LastUpdatedAt)
{
    public static JobStatusResponse From(JobTrackerQueryResponses.JobStatusResult result) =>
        Create(result.Id, result.SubmitterId, result.Progress, result.AssignedNode);

    public static JobStatusResponse From(JobTrackerNotifications.JobStatusChanged status) =>
        Create(status.Id, status.SubmitterId, status.Progress, status.AssignedNode);

    private static JobStatusResponse Create(
        JobId id,
        JobSubmitterId submitterId,
        JobProgress progress,
        Actor.Address? assignedNode) =>
        new(
            id.Value,
            submitterId.Value,
            progress.Status.ToString(),
            progress.Progress.Completed.Size,
            progress.Progress.Total.Size,
            progress.Progress.Fraction,
            assignedNode?.ToString(),
            progress.LastUpdatedAt);
}

/// <summary>A node's capacity as the tracker currently sees it.</summary>
public sealed record NodeResponse(
    string Address,
    string Status,
    bool Reachable,
    uint MaxCapacity,
    uint InUse,
    uint Available)
{
    public static NodeResponse From(NodeStatus node) => new(
        node.NodeAddress.ToString(),
        node.Status.ToString(),
        node.Reachable,
        node.MaximumCapacity.Size,
        node.CapacityInUse.Size,
        node.AvailableCapacity.Size);
}

/// <summary>A page of jobs.</summary>
public sealed record JobListResponse(IReadOnlyList<JobStatusResponse> Jobs, int Total)
{
    public static JobListResponse From(JobTrackerQueryResponses.JobList list) =>
        new([.. list.Jobs.Select(JobStatusResponse.From)], list.Total);
}

/// <summary>Queue depth and cluster capacity.</summary>
public sealed record QueueStatusResponse(
    int Waiting,
    int Running,
    uint QueuedWork,
    uint TotalCapacity,
    uint AvailableCapacity,
    IReadOnlyList<NodeResponse> Nodes)
{
    public static QueueStatusResponse From(JobTrackerQueryResponses.QueueStatus status) => new(
        status.WaitingCount,
        status.RunningCount,
        status.QueuedWork.Size,
        status.TotalCapacity.Size,
        status.AvailableCapacity.Size,
        [.. status.Nodes.Select(NodeResponse.From)]);
}

/// <summary>
/// Maps a domain rejection onto an HTTP status code.
/// </summary>
/// <remarks>
/// The mapping is one-to-one, which is a decent sign <see cref="JobRejectionReason"/> was carved
/// at the right joints — the domain never had HTTP in mind.
/// </remarks>
public static class RejectionMapping
{
    public static int ToStatusCode(JobRejectionReason reason) => reason switch
    {
        JobRejectionReason.DuplicateJobId => StatusCodes.Status409Conflict,
        JobRejectionReason.UnknownJob => StatusCodes.Status404NotFound,
        JobRejectionReason.AlreadyTerminal => StatusCodes.Status409Conflict,
        JobRejectionReason.NotSubmitter => StatusCodes.Status403Forbidden,
        JobRejectionReason.ExceedsClusterCapacity => StatusCodes.Status422UnprocessableEntity,

        // Only ever produced by worker reports, which never originate from HTTP.
        _ => StatusCodes.Status400BadRequest
    };
}
