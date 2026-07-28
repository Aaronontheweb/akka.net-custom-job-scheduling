namespace Akka.CustomJobScheduling.Core.Jobs;

/// <summary>
/// Describes job demand and worker capacity.
/// </summary>
/// <param name="Size">Underlying value.</param>
public readonly record struct JobSize(uint Size) : IComparable<JobSize>
{
    public static readonly JobSize Zero = new(0);

    public int CompareTo(JobSize other) => Size.CompareTo(other.Size);

    /// <summary>
    /// Subtracts <paramref name="amount"/>, clamping at <see cref="Zero"/>. The underlying value is
    /// unsigned, so plain subtraction would wrap around instead.
    /// </summary>
    public JobSize Reduce(JobSize amount)
    {
        if (this <= amount)
            return Zero;

        return this - amount;
    }

    /// <summary>
    /// Adds two sizes, clamping at <see cref="uint.MaxValue"/>. The underlying value is unsigned, so
    /// a plain sum would wrap around - the same hazard <see cref="Reduce"/> guards against on the way
    /// down. Capacity totals accumulate across every node and every queued job, so an unguarded sum
    /// could silently wrap to near-zero and make the scheduler believe there is no capacity left.
    /// </summary>
    public static JobSize operator +(JobSize left, JobSize right)
    {
        var sum = (ulong)left.Size + right.Size;

        return sum > uint.MaxValue ? new JobSize(uint.MaxValue) : new JobSize((uint)sum);
    }

    public static JobSize operator -(JobSize left, JobSize right) => new(left.Size - right.Size);

    public static bool operator <(JobSize left, JobSize right) => left.Size < right.Size;

    public static bool operator <=(JobSize left, JobSize right) => left.Size <= right.Size;

    public static bool operator >(JobSize left, JobSize right) => left.Size > right.Size;

    public static bool operator >=(JobSize left, JobSize right) => left.Size >= right.Size;
}
