using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Event;

namespace Akka.CustomJobScheduling.Core.Actors.Cluster;

/// <summary>
/// Subscribes to Akka.Cluster and republishes membership changes as job tracker facts.
/// </summary>
/// <remarks>
/// This is the only type in the codebase that knows <see cref="ClusterEvent"/> exists. Everything
/// downstream deals in <see cref="JobTrackerFacts.NodeJoined"/> and friends.
/// </remarks>
internal sealed class ClusterEventTranslator : ReceiveActor
{
    public static Props Props(IActorRef target, JobSize defaultCapacity, string workerRole) =>
        Actor.Props.Create(() => new ClusterEventTranslator(target, defaultCapacity, workerRole));

    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _target;
    private readonly JobSize _defaultCapacity;
    private readonly string _workerRole;

    public ClusterEventTranslator(IActorRef target, JobSize defaultCapacity, string workerRole)
    {
        _target = target;
        _defaultCapacity = defaultCapacity;
        _workerRole = workerRole;

        // CurrentClusterState arrives first and describes everyone already present, which is what
        // lets a tracker that starts late (or restarts) rebuild its node map without waiting for
        // the next membership change.
        // The snapshot describes the whole cluster, so it reconciles rather than announcing members
        // one at a time. That is what lets a tracker recovering from its journal discover that a
        // node it remembers has since left — no MemberRemoved is coming for it.
        Receive<ClusterEvent.CurrentClusterState>(state =>
        {
            var members = state.Members
                .Where(member => member.Status is MemberStatus.Up or MemberStatus.WeaklyUp)
                .Where(RunsWork)
                .ToImmutableDictionary(member => member.Address, _ => _defaultCapacity);

            _target.Tell(new JobTrackerFacts.NodesSynced(members));

            // Reconcile reachability for every worker member, not only the unreachable ones. A tracker
            // that recovered from its journal can hold a stale unreachable flag on a node that has
            // since healed - the ReachableMember event that would have cleared it went to the previous
            // tracker. Reporting the current verdict for each member corrects that; OnNodeReachability-
            // Changed is a no-op when the value is unchanged, so only the genuinely stale one flips.
            var unreachable = state.Unreachable
                .Select(member => member.Address)
                .ToImmutableHashSet();

            foreach (var member in state.Members)
            {
                Reachability(member, reachable: !unreachable.Contains(member.Address));
            }
        });

        Receive<ClusterEvent.MemberUp>(up => Join(up.Member));
        Receive<ClusterEvent.MemberWeaklyUp>(weaklyUp => Join(weaklyUp.Member));

        // MemberRemoved, not UnreachableMember — see JobTrackerState.OnNodeReachabilityChanged for
        // why requeueing has to wait for the cluster to commit to the node being gone.
        Receive<ClusterEvent.MemberRemoved>(removed =>
        {
            if (!RunsWork(removed.Member))
                return;

            _log.Info("Node {NodeAddress} removed from cluster; its work will be requeued",
                removed.Member.Address);
            _target.Tell(new JobTrackerFacts.NodeLeft(removed.Member.Address));
        });

        Receive<ClusterEvent.UnreachableMember>(unreachable =>
            Reachability(unreachable.Member, reachable: false));

        Receive<ClusterEvent.ReachableMember>(reachable =>
            Reachability(reachable.Member, reachable: true));
    }

    private void Join(Member member)
    {
        if (!RunsWork(member))
            return;

        _target.Tell(new JobTrackerFacts.NodeJoined(
            member.Address,
            member.Status,
            _defaultCapacity));
    }

    private void Reachability(Member member, bool reachable)
    {
        if (!RunsWork(member))
            return;

        _target.Tell(new JobTrackerFacts.NodeReachabilityChanged(member.Address, reachable));
    }

    /// <summary>
    /// Only nodes carrying the worker role can actually run jobs.
    /// </summary>
    /// <remarks>
    /// Without this filter every cluster member counts as capacity — including API nodes, which
    /// host no <see cref="JobReceiverActor"/>. The tracker would happily place work on one and the
    /// job would sit in <see cref="JobStatus.Running"/> at zero progress forever, because nothing
    /// there is listening for it.
    /// </remarks>
    private bool RunsWork(Member member) => member.HasRole(_workerRole);

    protected override void PreStart() =>
        Akka.Cluster.Cluster.Get(Context.System).Subscribe(
            Self,
            ClusterEvent.InitialStateAsSnapshot,
            typeof(ClusterEvent.IMemberEvent),
            typeof(ClusterEvent.IReachabilityEvent));

    protected override void PostStop() =>
        Akka.Cluster.Cluster.Get(Context.System).Unsubscribe(Self);
}
