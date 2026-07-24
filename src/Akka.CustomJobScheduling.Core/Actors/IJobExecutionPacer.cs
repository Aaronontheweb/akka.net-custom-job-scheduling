using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Controls how fast simulated work appears to run.
/// </summary>
/// <remarks>
/// Exists so the randomness stays out of <see cref="JobExecutorActor"/>. A test swaps in a pacer
/// that returns <see cref="TimeSpan.Zero"/> and gets a deterministic executor that finishes
/// immediately; production gets a random one that makes bigger jobs take longer.
/// </remarks>
public interface IJobExecutionPacer
{
    /// <summary>How long until the next chunk of work completes.</summary>
    TimeSpan NextInterval(JobDefinition job);

    /// <summary>How much work completes per tick. Must be greater than zero or the job never ends.</summary>
    JobSize TickSize(JobDefinition job);
}

/// <summary>
/// Simulates work by sleeping a random interval per tick, so larger jobs take proportionally longer.
/// </summary>
public sealed class RandomJobExecutionPacer : IJobExecutionPacer
{
    private readonly TimeSpan _minInterval;
    private readonly TimeSpan _maxInterval;
    private readonly uint _tickSize;

    public RandomJobExecutionPacer(
        TimeSpan? minInterval = null,
        TimeSpan? maxInterval = null,
        uint tickSize = 1)
    {
        _minInterval = minInterval ?? TimeSpan.FromMilliseconds(250);
        _maxInterval = maxInterval ?? TimeSpan.FromMilliseconds(1500);
        _tickSize = Math.Max(1, tickSize);
    }

    public TimeSpan NextInterval(JobDefinition job)
    {
        var spread = _maxInterval - _minInterval;

        if (spread <= TimeSpan.Zero)
            return _minInterval;

        return _minInterval + Random.Shared.NextDouble() * spread;
    }

    public JobSize TickSize(JobDefinition job) => new(_tickSize);
}
