namespace Akka.CustomJobScheduling.Core.Actors;

/*
 * Marker types used as ActorRegistry keys.
 *
 * These are deliberately not the actor classes themselves. In Clustered mode the registry holds a
 * singleton proxy or a shard region — not the actor — so keying on the actor type would be a lie.
 * A dedicated marker keeps `registry.Get<JobTrackerKey>()` meaning "however you reach the tracker
 * in this execution mode", which is exactly what callers want.
 */

/// <summary>
/// Registry key for the job tracker. Resolves to the actor itself in
/// <see cref="AkkaExecutionMode.LocalTest"/>, or to a cluster singleton proxy in
/// <see cref="AkkaExecutionMode.Clustered"/>.
/// </summary>
public sealed class JobTrackerKey;

/// <summary>
/// Registry key for this node's job receiver.
/// </summary>
public sealed class JobReceiverKey;

/// <summary>
/// Registry key for the job submitter parent — a <see cref="GenericChildPerEntityParent"/> locally,
/// a shard region when clustered.
/// </summary>
public sealed class JobSubmitterManagerKey;

/// <summary>
/// Registry key for this node's job feed supervisor. Always node-local; there is no clustered
/// variant, because the feeds it owns terminate HTTP connections on this node.
/// </summary>
public sealed class JobStreamSupervisorKey;
