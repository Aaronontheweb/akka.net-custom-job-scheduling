using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Tests;

public class NodeCapacityComparerTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public static TheoryData<MemberStatus, bool, uint, uint, bool> EligibilityCases => new()
    {
        { MemberStatus.Up, true, 100, 20, true },
        { MemberStatus.WeaklyUp, true, 100, 20, true },
        { MemberStatus.Joining, true, 100, 20, false },
        { MemberStatus.Leaving, true, 100, 20, false },
        { MemberStatus.Exiting, true, 100, 20, false },
        { MemberStatus.Down, true, 100, 20, false },
        { MemberStatus.Up, false, 100, 20, false },
        { MemberStatus.Up, true, 100, 90, false }
    };

    [Theory]
    [MemberData(nameof(EligibilityCases))]
    public void Node_eligibility_requires_availability_and_capacity(
        MemberStatus status,
        bool reachable,
        uint maximum,
        uint inUse,
        bool expected)
    {
        var node = Node("node", status, reachable, maximum, inUse);

        Assert.Equal(expected, node.CanAccept(Job("job", 20)));
    }

    [Theory]
    [InlineData(20u, 60u, "node-a")]
    [InlineData(60u, 20u, "node-b")]
    [InlineData(20u, 20u, "node-a")]
    public void Capacity_comparer_orders_most_available_first(
        uint firstInUse,
        uint secondInUse,
        string expectedHost)
    {
        var candidates = new[]
        {
            Node("node-b", MemberStatus.Up, true, 100, secondInUse),
            Node("node-a", MemberStatus.Up, true, 100, firstInUse)
        };

        var selected = candidates
            .OrderBy(node => node, MostAvailableCapacityComparer.Instance)
            .First();

        Assert.Equal(expectedHost, selected.NodeAddress.Host);
    }

    private static JobDefinition Job(string id, uint size) =>
        new(new JobId(id), new JobSize(size));

    private static NodeStatus Node(
        string host,
        MemberStatus status,
        bool reachable,
        uint maximum,
        uint inUse) =>
        new(
            new Address("akka", "JobSystem", host, 2552),
            status,
            reachable,
            Now,
            new JobSize(maximum),
            new JobSize(inUse));
}
