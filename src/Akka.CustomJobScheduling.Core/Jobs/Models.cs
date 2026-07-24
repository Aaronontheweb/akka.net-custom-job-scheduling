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
    public JobSize AvailableCapacity =>
        CapacityInUse >= MaximumCapacity ? JobSize.Zero : MaximumCapacity - CapacityInUse;

    public bool HasCapacityFor(JobDefinition job) => AvailableCapacity >= job.Size;

    public bool CanAccept(JobDefinition job)
    {
        return Status is MemberStatus.Up or MemberStatus.WeaklyUp
               && Reachable
               && HasCapacityFor(job);
    }
}

/// <summary>
/// What is the execution status of a given job?
/// </summary>
public enum JobStatus
{
    Waiting = 0,
    Running = 1,
    Faulted = 2,
    Completed = 3
}

/// <summary>
/// The execution status and observed progress of a job.
/// </summary>
public sealed record JobProgress(
    JobId Id,
    JobStatus Status,
    WorkProgress Progress,
    DateTimeOffset LastUpdatedAt) : IWithJobId;
