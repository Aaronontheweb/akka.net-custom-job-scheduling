using Akka.Actor;
using Akka.Cluster.Sharding;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// A generic "child per entity" parent actor.
/// </summary>
/// <remarks>
/// Intended for simplifying unit tests where we don't want to use Akka.Cluster.Sharding. Re-uses
/// Akka.Cluster.Sharding's <see cref="IMessageExtractor"/> so the routing rules are identical in
/// both execution modes — the same extractor instance works here and in a real shard region.
/// </remarks>
public sealed class GenericChildPerEntityParent : ReceiveActor
{
    public static Props CreateProps(IMessageExtractor extractor, Func<string, Props> propsFactory)
    {
        return Props.Create(() => new GenericChildPerEntityParent(extractor, propsFactory));
    }

    private readonly IMessageExtractor _extractor;

    public GenericChildPerEntityParent(IMessageExtractor extractor, Func<string, Props> propsFactory)
    {
        _extractor = extractor;

        ReceiveAny(o =>
        {
            var entityId = _extractor.EntityId(o);
            if (entityId is null)
                return;

            Context.Child(entityId)
                .GetOrElse(() => Context.ActorOf(propsFactory(entityId), entityId))
                .Forward(_extractor.EntityMessage(o));
        });
    }
}
