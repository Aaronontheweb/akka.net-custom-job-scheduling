using Akka.Actor;
using Akka.Cluster;

namespace Akka.CustomJobScheduling.Core.Jobs;

/// <summary>
/// Describes a job to be done. In a production system this would likely include some configuration parameters
/// or specifications, but we're keeping it simple here.
/// </summary>
/// <param name="Id">Unique job identifier.</param>
/// <param name="Size">Total size of the job.</param>
public sealed record JobDefinition(JobId Id, JobSize Size) : IWithJobId;

/// <summary>
/// The amount of total capacity defined on each node.
/// </summary>
/// <param name="NodeAddress">The Akka.NET Address for this node.</param>
/// <param name="MaxCapacity">Maximum cumulative work units that can run in parallel.</param>
public sealed record NodeCapacity(Address NodeAddress, JobSize MaxCapacity);

/// <summary>
/// Describes the allocation burden + Akka.Cluster status of a given worker node.
/// </summary>
public sealed record NodeStatus(
    Address NodeAddress,
    MemberStatus Status,
    bool Reachable,
    DateTimeOffset LastUpdatedAt,
    JobSize MaximumCapacity,
    JobSize CapacityInUse)
{
    public JobSize AvailableCapacity => MaximumCapacity.Reduce(CapacityInUse);

    /// <summary>
    /// Whether this node is in a state where it may be handed new work at all.
    /// </summary>
    public bool IsEligible => Status is MemberStatus.Up or MemberStatus.WeaklyUp && Reachable;

    public bool HasCapacityFor(JobDefinition job) => AvailableCapacity >= job.Size;

    public bool CanAccept(JobDefinition job) => IsEligible && HasCapacityFor(job);

    /// <summary>
    /// A node that has just joined the cluster with nothing running on it yet.
    /// </summary>
    public static NodeStatus Joined(
        Address address,
        MemberStatus status,
        JobSize maxCapacity,
        DateTimeOffset joinedAt) =>
        new(address, status, Reachable: true, joinedAt, maxCapacity, JobSize.Zero);

    public NodeStatus WithStatus(MemberStatus status, bool reachable, DateTimeOffset at) =>
        this with { Status = status, Reachable = reachable, LastUpdatedAt = at };

    /// <summary>Commits capacity to a job placed on this node.</summary>
    public NodeStatus Reserve(JobSize amount) =>
        this with { CapacityInUse = CapacityInUse + amount };

    /// <summary>Gives capacity back once a job leaves this node.</summary>
    public NodeStatus Release(JobSize amount) =>
        this with { CapacityInUse = CapacityInUse.Reduce(amount) };
}

/// <summary>
/// What is the execution status of a given job?
/// </summary>
public enum JobStatus
{
    Waiting = 0,
    Running = 1,
    Faulted = 2,
    Completed = 3,

    /// <summary>
    /// Withdrawn by the submitter before it finished.
    /// </summary>
    Cancelled = 4
}

/// <summary>
/// The execution status and observed progress of a job.
/// </summary>
public sealed record JobProgress(
    JobId Id,
    JobStatus Status,
    WorkProgress Progress,
    DateTimeOffset LastUpdatedAt) : IWithJobId;
