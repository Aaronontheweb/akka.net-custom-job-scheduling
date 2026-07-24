using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Tests;

public class WorkValueTests
{
    [Theory]
    [InlineData(0u, 0u, 0u)]
    [InlineData(10u, 4u, 14u)]
    [InlineData(uint.MaxValue - 1u, 1u, uint.MaxValue)]
    public void JobSize_adds(uint left, uint right, uint expected)
    {
        Assert.Equal(new JobSize(expected), new JobSize(left) + new JobSize(right));
    }

    [Theory]
    [InlineData(10u, 4u, 6u)]
    [InlineData(4u, 4u, 0u)]
    public void JobSize_subtracts(uint left, uint right, uint expected)
    {
        Assert.Equal(new JobSize(expected), new JobSize(left) - new JobSize(right));
    }

    [Theory]
    [InlineData(0u, 0u)]
    [InlineData(2u, 5u)]
    [InlineData(5u, 5u)]
    public void WorkProgress_calculates_fraction(uint completed, uint total)
    {
        var progress = new WorkProgress(new JobSize(completed), new JobSize(total));
        var expected = total == 0 ? 0m : (decimal)completed / total;

        Assert.Equal(expected, progress.Fraction);
    }
}
