using System.Text.Json.Serialization;

namespace Akka.CustomJobScheduling.Core.Jobs;

/// <summary>
/// Numeric progress through a job.
/// </summary>
public readonly record struct WorkProgress(JobSize Completed, JobSize Total)
{
    [JsonIgnore]
    public decimal Fraction
    {
        get
        {
            if (Total == JobSize.Zero)
                return 0m;

            return (decimal)Completed.Size / Total.Size;
        }
    }

    /// <summary>Nothing done yet.</summary>
    public static WorkProgress None(JobSize total) => new(JobSize.Zero, total);

    /// <summary>All the way through.</summary>
    public static WorkProgress Full(JobSize total) => new(total, total);
}
