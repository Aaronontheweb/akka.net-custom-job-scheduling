using System.Collections.Concurrent;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Membership source backed by real Akka.Cluster events. Used in
/// <see cref="AkkaExecutionMode.Clustered"/>.
/// </summary>
/// <remarks>
/// Each subscriber gets its own <see cref="ClusterEventTranslator"/> so that it receives a
/// <c>CurrentClusterState</c> snapshot at subscribe time rather than only seeing changes that
/// happen to occur afterwards.
/// </remarks>
public sealed class ClusterMembershipSource : IClusterMembershipSource
{
    private readonly ActorSystem _system;
    private readonly JobSize _defaultCapacity;
    private readonly ConcurrentDictionary<IActorRef, IActorRef> _translators = new();

    public ClusterMembershipSource(ActorSystem system, JobSize defaultCapacity)
    {
        _system = system;
        _defaultCapacity = defaultCapacity;
    }

    public void Subscribe(IActorRef subscriber)
    {
        _translators.GetOrAdd(
            subscriber,
            target => _system.ActorOf(ClusterEventTranslator.Props(target, _defaultCapacity)));
    }

    public void Unsubscribe(IActorRef subscriber)
    {
        if (_translators.TryRemove(subscriber, out var translator))
            _system.Stop(translator);
    }
}
