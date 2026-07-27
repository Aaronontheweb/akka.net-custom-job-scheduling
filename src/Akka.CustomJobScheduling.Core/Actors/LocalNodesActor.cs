using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors.Cluster;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Hosts one <see cref="JobReceiverActor"/> per fake node in
/// <see cref="AkkaExecutionMode.LocalTest"/>.
/// </summary>
/// <remarks>
/// Reproduces the actor path layout that exists naturally across a real cluster —
/// <c>/user/nodes/{host}/job-receiver</c> — inside a single <see cref="ActorSystem"/>, so that
/// <see cref="LocalJobReceiverRouter"/> can resolve a receiver from a node
/// <see cref="Address"/> using the same lookup shape as the remote router. That is what lets one
/// test process behave like a multi-node cluster.
/// </remarks>
public sealed class LocalNodesActor : ReceiveActor
{
    public static Props Props(
        IReadOnlyList<string> hosts,
        IActorRef tracker,
        IJobExecutionPacer pacer) =>
        Actor.Props.Create(() => new LocalNodesActor(hosts, tracker, pacer));

    public LocalNodesActor(IReadOnlyList<string> hosts, IActorRef tracker, IJobExecutionPacer pacer)
    {
        foreach (var host in hosts)
        {
            var address = LocalClusterMembershipSource.NodeAddress(host);
            Context.ActorOf(LocalNodeActor.Props(address, tracker, pacer), host);
        }

        ReceiveAny(_ => { });
    }
}

/// <summary>
/// Stands in for one node. Exists only to own a child named
/// <see cref="JobReceiverActor.Name"/> at the right depth in the path.
/// </summary>
internal sealed class LocalNodeActor : ReceiveActor
{
    public static Props Props(Address address, IActorRef tracker, IJobExecutionPacer pacer) =>
        Actor.Props.Create(() => new LocalNodeActor(address, tracker, pacer));

    public LocalNodeActor(Address address, IActorRef tracker, IJobExecutionPacer pacer)
    {
        Context.ActorOf(JobReceiverActor.Props(address, tracker, pacer), JobReceiverActor.Name);
        ReceiveAny(_ => { });
    }
}
