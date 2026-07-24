namespace Akka.CustomJobScheduling.Core.Jobs;

/// <summary>
/// Numeric progress through a job.
/// </summary>
public readonly record struct WorkProgress(JobSize Completed, JobSize Total)
{
    public decimal Fraction => Total == JobSize.Zero ? 0m : (decimal)Completed.Size / Total.Size;
}
