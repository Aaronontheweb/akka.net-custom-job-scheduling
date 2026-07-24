using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Subscribes to Akka.Cluster and republishes membership changes as job tracker commands.
/// </summary>
/// <remarks>
/// This is the only type in the codebase that knows <see cref="ClusterEvent"/> exists. Everything
/// downstream deals in <see cref="JobTrackerCommands.NodeJoined"/> and friends.
/// </remarks>
internal sealed class ClusterEventTranslator : ReceiveActor
{
    public static Props Props(IActorRef target, JobSize defaultCapacity) =>
        Actor.Props.Create(() => new ClusterEventTranslator(target, defaultCapacity));

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _target;
    private readonly JobSize _defaultCapacity;

    public ClusterEventTranslator(IActorRef target, JobSize defaultCapacity)
    {
        _target = target;
        _defaultCapacity = defaultCapacity;

        // CurrentClusterState arrives first and describes everyone already present, which is what
        // lets a tracker that starts late (or restarts) rebuild its node map without waiting for
        // the next membership change.
        Receive<ClusterEvent.CurrentClusterState>(state =>
        {
            foreach (var member in state.Members)
            {
                if (member.Status is MemberStatus.Up or MemberStatus.WeaklyUp)
                    Join(member);
            }

            foreach (var unreachable in state.Unreachable)
            {
                _target.Tell(new JobTrackerCommands.NodeReachabilityChanged(
                    unreachable.Address,
                    Reachable: false));
            }
        });

        Receive<ClusterEvent.MemberUp>(up => Join(up.Member));
        Receive<ClusterEvent.MemberWeaklyUp>(weaklyUp => Join(weaklyUp.Member));

        // MemberRemoved, not UnreachableMember — see JobTrackerState.OnNodeReachabilityChanged for
        // why requeueing has to wait for the cluster to commit to the node being gone.
        Receive<ClusterEvent.MemberRemoved>(removed =>
        {
            _log.Info("Node {NodeAddress} removed from cluster; its work will be requeued",
                removed.Member.Address);
            _target.Tell(new JobTrackerCommands.NodeLeft(removed.Member.Address));
        });

        Receive<ClusterEvent.UnreachableMember>(unreachable =>
            _target.Tell(new JobTrackerCommands.NodeReachabilityChanged(
                unreachable.Member.Address,
                Reachable: false)));

        Receive<ClusterEvent.ReachableMember>(reachable =>
            _target.Tell(new JobTrackerCommands.NodeReachabilityChanged(
                reachable.Member.Address,
                Reachable: true)));
    }

    private void Join(Member member) =>
        _target.Tell(new JobTrackerCommands.NodeJoined(
            member.Address,
            member.Status,
            _defaultCapacity));

    protected override void PreStart() =>
        Akka.Cluster.Cluster.Get(Context.System).Subscribe(
            Self,
            ClusterEvent.InitialStateAsSnapshot,
            typeof(ClusterEvent.IMemberEvent),
            typeof(ClusterEvent.IReachabilityEvent));

    protected override void PostStop() =>
        Akka.Cluster.Cluster.Get(Context.System).Unsubscribe(Self);
}
