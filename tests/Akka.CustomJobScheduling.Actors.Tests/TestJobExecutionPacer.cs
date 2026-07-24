using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Actors.Tests;

/// <summary>
/// A pacer tests can steer.
/// </summary>
/// <remarks>
/// Defaults to a tick interval long enough that nothing ever finishes, so a job stays
/// <see cref="JobStatus.Running"/> and holds its capacity for the duration of a test. Tests that
/// want completion set <see cref="Interval"/> to zero; tests that want to observe intermediate
/// progress also set <see cref="UnitsPerTick"/>.
/// </remarks>
public sealed class TestJobExecutionPacer : IJobExecutionPacer
{
    /// <summary>Long enough that a job never advances unless a test asks it to.</summary>
    public static readonly TimeSpan Never = TimeSpan.FromMinutes(10);

    public TimeSpan Interval { get; set; } = Never;

    /// <summary>Work units per tick; <c>null</c> finishes the whole job in one tick.</summary>
    public uint? UnitsPerTick { get; set; }

    /// <summary>Run jobs to completion as fast as the mailbox allows.</summary>
    public TestJobExecutionPacer RunToCompletion(uint? unitsPerTick = null)
    {
        Interval = TimeSpan.Zero;
        UnitsPerTick = unitsPerTick;
        return this;
    }

    public TimeSpan NextInterval(JobDefinition job) => Interval;

    public JobSize TickSize(JobDefinition job) =>
        new(UnitsPerTick ?? Math.Max(1u, job.Size.Size));
}
