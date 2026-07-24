namespace Akka.CustomerJobScheduling.Core.Jobs;

/// <summary>
/// Represents a unique job execution inside the platform.
/// </summary>
/// <param name="Value">the string representation of the unique key.</param>
public readonly record struct JobId(string Value);

/// <summary>
/// Marker interface for all messages / state pertaining to job submission and progress.
/// </summary>
public interface IWithJobId
{
    JobId Id { get; }
}