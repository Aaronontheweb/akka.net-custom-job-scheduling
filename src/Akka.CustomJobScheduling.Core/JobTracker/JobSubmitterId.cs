namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Value object for tracking WHO offered to submit a job.
/// </summary>
/// <param name="Value">The job submitter's id represented as a string.</param>
public readonly record struct JobSubmitterId(string Value);

public interface IWithJobSubmitterId
{
    JobSubmitterId SubmitterId { get; }
}