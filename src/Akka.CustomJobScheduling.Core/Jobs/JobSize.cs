namespace Akka.CustomJobScheduling.Core.Jobs;

/// <summary>
/// Describes job demand and worker capacity.
/// </summary>
/// <param name="Size">Underlying value.</param>
public readonly record struct JobSize(uint Size) : IComparable<JobSize>
{
    public static readonly JobSize Zero = new(0);

    public int CompareTo(JobSize other) => Size.CompareTo(other.Size);

    public static JobSize operator +(JobSize left, JobSize right) => new(left.Size + right.Size);

    public static JobSize operator -(JobSize left, JobSize right) => new(left.Size - right.Size);

    public static bool operator <(JobSize left, JobSize right) => left.Size < right.Size;

    public static bool operator <=(JobSize left, JobSize right) => left.Size <= right.Size;

    public static bool operator >(JobSize left, JobSize right) => left.Size > right.Size;

    public static bool operator >=(JobSize left, JobSize right) => left.Size >= right.Size;
}
