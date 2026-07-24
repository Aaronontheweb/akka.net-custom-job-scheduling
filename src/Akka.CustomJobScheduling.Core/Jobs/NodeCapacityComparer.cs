namespace Akka.CustomJobScheduling.Core.Jobs;

public sealed class MostAvailableCapacityComparer : IComparer<NodeStatus>
{
    public static readonly MostAvailableCapacityComparer Instance = new();

    private MostAvailableCapacityComparer()
    {
    }

    public int Compare(NodeStatus? x, NodeStatus? y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (x is null)
            return 1;
        if (y is null)
            return -1;

        var capacityComparison = y.AvailableCapacity.CompareTo(x.AvailableCapacity);
        return capacityComparison != 0
            ? capacityComparison
            : string.CompareOrdinal(x.NodeAddress.ToString(), y.NodeAddress.ToString());
    }
}
