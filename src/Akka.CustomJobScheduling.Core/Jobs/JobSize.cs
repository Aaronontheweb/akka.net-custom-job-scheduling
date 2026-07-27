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

    public static JobSize operator +(JobSize left, JobSize right) => new(left.Size + right.Size);

    public static JobSize operator -(JobSize left, JobSize right) => new(left.Size - right.Size);

    public static bool operator <(JobSize left, JobSize right) => left.Size < right.Size;

    public static bool operator <=(JobSize left, JobSize right) => left.Size <= right.Size;

    public static bool operator >(JobSize left, JobSize right) => left.Size > right.Size;

    public static bool operator >=(JobSize left, JobSize right) => left.Size >= right.Size;
}
