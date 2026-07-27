namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Determines how Akka.Hosting configures cluster features.
/// </summary>
public enum AkkaExecutionMode
{
    /// <summary>
    /// Local testing mode — no remoting, no clustering, no management. Uses
    /// <see cref="GenericChildPerEntityParent"/> for entity routing and a hand-driven membership
    /// source. Fast startup, no network overhead.
    /// </summary>
    LocalTest,

    /// <summary>
    /// Full cluster mode with sharding, singletons, and real Akka.Cluster membership events.
    /// </summary>
    Clustered
}
