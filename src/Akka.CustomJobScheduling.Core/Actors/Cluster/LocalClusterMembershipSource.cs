using System.Collections.Immutable;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Membership source with no cluster behind it — topology is driven by hand. Used in
/// <see cref="AkkaExecutionMode.LocalTest"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is what makes the whole system testable without a cluster. A test calls
/// <see cref="MemberUp(string, uint)"/> twice and the tracker believes it has two worker nodes;
/// a test calls <see cref="MemberRemoved(string)"/> mid-job and the tracker requeues exactly as it
/// would after a real downing. No remoting, no seed nodes, no waiting for gossip to converge.
/// </para>
/// <para>
/// It holds current membership rather than a log of what was said, and reconciles on subscribe —
/// the same shape as the real source, which reports <c>CurrentClusterState</c> when an actor
/// subscribes. That equivalence matters: a tracker recovering from its journal has to learn about
/// nodes that vanished while it was down, and it can only do that from a full picture.
/// </para>
/// </remarks>
public sealed class LocalClusterMembershipSource : IClusterMembershipSource
{
    private readonly HashSet<IActorRef> _subscribers = [];
    private readonly Dictionary<Address, JobSize> _members = [];
    private readonly HashSet<Address> _unreachable = [];
    private readonly Lock _gate = new();

    /// <summary>
    /// A synthetic address for a fake node. Local mode has no remoting, so these are labels rather
    /// than reachable endpoints — <see cref="LocalJobReceiverRouter"/> maps them back to
    /// locally-hosted receivers by host name.
    /// </summary>
    public static Address NodeAddress(string host) => new("akka", "JobScheduling", host, 2552);

    public void Subscribe(IActorRef subscriber)
    {
        JobTrackerCommands.SyncNodes sync;
        Address[] unreachable;

        lock (_gate)
        {
            if (!_subscribers.Add(subscriber))
                return;

            sync = new JobTrackerCommands.SyncNodes(_members.ToImmutableDictionary());
            unreachable = [.. _unreachable];
        }

        subscriber.Tell(sync);

        foreach (var address in unreachable)
        {
            subscriber.Tell(new JobTrackerCommands.NodeReachabilityChanged(address, Reachable: false));
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
    public void MemberUp(Address address, JobSize capacity)
    {
        lock (_gate)
        {
            _members[address] = capacity;
            _unreachable.Remove(address);
        }

        Publish(new JobTrackerCommands.NodeJoined(address, Akka.Cluster.MemberStatus.Up, capacity));
    }

    /// <summary>A node joined and is ready for work.</summary>
    public void MemberUp(string host, uint capacity) =>
        MemberUp(NodeAddress(host), new JobSize(capacity));

    /// <summary>A node was downed and removed. Its work gets requeued.</summary>
    public void MemberRemoved(Address address)
    {
        Forget(address);
        Publish(new JobTrackerCommands.NodeLeft(address));
    }

    /// <summary>A node was downed and removed. Its work gets requeued.</summary>
    public void MemberRemoved(string host) => MemberRemoved(NodeAddress(host));

    /// <summary>The failure detector lost sight of a node. Its work stays put.</summary>
    public void Unreachable(Address address)
    {
        lock (_gate)
        {
            _unreachable.Add(address);
        }

        Publish(new JobTrackerCommands.NodeReachabilityChanged(address, Reachable: false));
    }

    /// <summary>The failure detector lost sight of a node. Its work stays put.</summary>
    public void Unreachable(string host) => Unreachable(NodeAddress(host));

    /// <summary>The partition healed.</summary>
    public void Reachable(Address address)
    {
        lock (_gate)
        {
            _unreachable.Remove(address);
        }

        Publish(new JobTrackerCommands.NodeReachabilityChanged(address, Reachable: true));
    }

    /// <summary>The partition healed.</summary>
    public void Reachable(string host) => Reachable(NodeAddress(host));

    /// <summary>
    /// Drops a node from membership without telling anyone.
    /// </summary>
    /// <remarks>
    /// Models a node that left while nobody was listening. A real cluster delivers
    /// <c>MemberRemoved</c> to whoever is subscribed at the time, and a tracker that was down never
    /// hears it — all a restarted subscriber gets is the current member set. This is the only way
    /// to reproduce that from a test.
    /// </remarks>
    public void Forget(Address address)
    {
        lock (_gate)
        {
            _members.Remove(address);
            _unreachable.Remove(address);
        }
    }

    /// <inheritdoc cref="Forget(Address)"/>
    public void Forget(string host) => Forget(NodeAddress(host));

    private void Publish(IJobTrackerCommand command)
    {
        IActorRef[] targets;

        lock (_gate)
        {
            targets = [.. _subscribers];
        }

        foreach (var subscriber in targets)
        {
            subscriber.Tell(command);
        }
    }
}
