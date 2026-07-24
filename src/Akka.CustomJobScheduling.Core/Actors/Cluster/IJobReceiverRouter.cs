using Akka.Actor;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// How the tracker reaches the <see cref="JobReceiverActor"/> on a given node.
/// </summary>
/// <remarks>
/// The tracker only ever holds an <see cref="Address"/> — it has no <see cref="IActorRef"/> for a
/// remote receiver and shouldn't be in the business of building actor paths. Abstracting this is
/// what lets several fake "nodes" live inside one <see cref="ActorSystem"/> during tests while the
/// tracker's own code path stays identical.
/// </remarks>
public interface IJobReceiverRouter
{
    ActorSelection Select(Address nodeAddress);
}

/// <summary>
/// Resolves receivers at their remote actor path. Used in
/// <see cref="AkkaExecutionMode.Clustered"/>.
/// </summary>
public sealed class RemoteJobReceiverRouter : IJobReceiverRouter
{
    private readonly ActorSystem _system;

    public RemoteJobReceiverRouter(ActorSystem system)
    {
        _system = system;
    }

    public ActorSelection Select(Address nodeAddress) =>
        _system.ActorSelection($"{nodeAddress}/user/{JobReceiverActor.Name}");
}

/// <summary>
/// Resolves receivers hosted locally under <c>/user/nodes/{host}</c>. Used in
/// <see cref="AkkaExecutionMode.LocalTest"/>.
/// </summary>
public sealed class LocalJobReceiverRouter : IJobReceiverRouter
{
    /// <summary>Parent path under which local stand-in nodes are hosted.</summary>
    public const string NodesPath = "/user/nodes";

    private readonly ActorSystem _system;

    public LocalJobReceiverRouter(ActorSystem system)
    {
        _system = system;
    }

    public static string PathFor(Address nodeAddress) =>
        $"{NodesPath}/{nodeAddress.Host}/{JobReceiverActor.Name}";

    public ActorSelection Select(Address nodeAddress) =>
        _system.ActorSelection(PathFor(nodeAddress));
}
