using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Membership source with no cluster behind it — topology is driven by hand. Used in
/// <see cref="AkkaExecutionMode.LocalTest"/>.
/// </summary>
/// <remarks>
/// This is what makes the whole system testable without a cluster. A test calls
/// <see cref="MemberUp"/> twice and the tracker believes it has two worker nodes; a test calls
/// <see cref="MemberRemoved"/> mid-job and the tracker requeues exactly as it would after a real
/// downing. No remoting, no seed nodes, no waiting for gossip to converge.
/// </remarks>
public sealed class LocalClusterMembershipSource : IClusterMembershipSource
{
    private readonly HashSet<IActorRef> _subscribers = [];
    private readonly List<IJobTrackerCommand> _history = [];
    private readonly Lock _gate = new();

    /// <summary>
    /// A synthetic address for a fake node. Local mode has no remoting, so these are labels rather
    /// than reachable endpoints — the <see cref="LocalJobReceiverRouter"/> maps them back to
    /// locally-hosted receivers by host name.
    /// </summary>
    public static Address NodeAddress(string host) => new("akka", "JobScheduling", host, 2552);

    public void Subscribe(IActorRef subscriber)
    {
        lock (_gate)
        {
            if (!_subscribers.Add(subscriber))
                return;

            // Replay what has already happened, mirroring the CurrentClusterState snapshot a real
            // subscriber receives. Without this, a tracker that starts after the nodes do would
            // never learn they exist.
            foreach (var command in _history)
            {
                subscriber.Tell(command);
            }
        }
    }

    public void Unsubscribe(IActorRef subscriber)
    {
        lock (_gate)
        {
            _subscribers.Remove(subscriber);
        }
    }

    /// <summary>A node joined and is ready for work.</summary>
    public void MemberUp(Address address, JobSize capacity) =>
        Publish(new JobTrackerCommands.NodeJoined(address, MemberStatus.Up, capacity));

    /// <summary>A node joined and is ready for work.</summary>
    public void MemberUp(string host, uint capacity) =>
        MemberUp(NodeAddress(host), new JobSize(capacity));

    /// <summary>A node was downed and removed. Its work gets requeued.</summary>
    public void MemberRemoved(Address address) =>
        Publish(new JobTrackerCommands.NodeLeft(address));

    /// <summary>A node was downed and removed. Its work gets requeued.</summary>
    public void MemberRemoved(string host) => MemberRemoved(NodeAddress(host));

    /// <summary>The failure detector lost sight of a node. Its work stays put.</summary>
    public void Unreachable(Address address) =>
        Publish(new JobTrackerCommands.NodeReachabilityChanged(address, Reachable: false));

    /// <summary>The failure detector lost sight of a node. Its work stays put.</summary>
    public void Unreachable(string host) => Unreachable(NodeAddress(host));

    /// <summary>The partition healed.</summary>
    public void Reachable(Address address) =>
        Publish(new JobTrackerCommands.NodeReachabilityChanged(address, Reachable: true));

    /// <summary>The partition healed.</summary>
    public void Reachable(string host) => Reachable(NodeAddress(host));

    private void Publish(IJobTrackerCommand command)
    {
        IActorRef[] targets;

        lock (_gate)
        {
            _history.Add(command);
            targets = [.. _subscribers];
        }

        foreach (var subscriber in targets)
        {
            subscriber.Tell(command);
        }
    }
}
