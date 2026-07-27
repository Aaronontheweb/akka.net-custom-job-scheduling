using Akka.Actor;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Where the job tracker learns about worker nodes.
/// </summary>
/// <remarks>
/// <para>
/// This exists purely so the tracker never touches <c>Cluster.Get(system)</c>. Subscribers receive
/// <see cref="JobTracker.JobTrackerCommands.NodeJoined"/>,
/// <see cref="JobTracker.JobTrackerCommands.NodeLeft"/>, and
/// <see cref="JobTracker.JobTrackerCommands.NodeReachabilityChanged"/> — all of which are already
/// cluster-agnostic, so a fake can produce them without any Akka.Cluster infrastructure at all.
/// </para>
/// <para>
/// That is the whole trick behind testing this system locally: the topology that would normally
/// take a multi-node cluster to produce becomes three method calls on a fake.
/// </para>
/// </remarks>
public interface IClusterMembershipSource
{
    /// <summary>
    /// Begin delivering membership commands to <paramref name="subscriber"/>.
    /// </summary>
    void Subscribe(IActorRef subscriber);

    /// <summary>
    /// Stop delivering membership commands to <paramref name="subscriber"/>.
    /// </summary>
    void Unsubscribe(IActorRef subscriber);
}
