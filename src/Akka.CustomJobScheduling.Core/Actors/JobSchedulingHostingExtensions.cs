using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.DependencyInjection;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.CustomJobScheduling.Core.Actors;

/// <summary>
/// Akka.Hosting registration for the job scheduling domain.
/// </summary>
/// <remarks>
/// Every actor gets a pair of registrations that differ only in the infrastructure underneath, and
/// both register under the same <c>...Key</c> marker. Call sites — including tests — never learn
/// which mode they're in.
/// </remarks>
public static class JobSchedulingHostingExtensions
{
    /// <summary>Default per-node capacity when configuration doesn't say otherwise.</summary>
    public const uint DefaultNodeCapacity = 100;

    /// <summary>Cluster role that hosts workers and the tracker singleton.</summary>
    public const string WorkerRole = "worker";

    /// <summary>Name of the submitter shard region, and of its local stand-in.</summary>
    public const string SubmitterRegionName = "job-submitters";

    /// <summary>Top-level actor that owns the fake nodes in local mode.</summary>
    public const string LocalNodesName = "nodes";

    /// <summary>
    /// Registers the services the actors depend on, choosing implementations by execution mode.
    /// </summary>
    /// <remarks>
    /// Anything registered before this call wins — tests override <see cref="IJobExecutionPacer"/>
    /// and <see cref="TimeProvider"/> this way, and can substitute
    /// <see cref="LocalClusterMembershipSource"/> with an instance they hold a reference to so they
    /// can drive topology by hand.
    /// </remarks>
    public static IServiceCollection AddJobSchedulingServices(
        this IServiceCollection services,
        AkkaExecutionMode executionMode,
        uint nodeCapacity = DefaultNodeCapacity)
    {
        if (executionMode == AkkaExecutionMode.Clustered)
        {
            services.TryAddSingletonSource<IClusterMembershipSource>(sp =>
                new ClusterMembershipSource(
                    sp.GetRequiredService<ActorSystem>(),
                    new JobSize(nodeCapacity)));

            services.TryAddSingletonSource<IJobReceiverRouter>(sp =>
                new RemoteJobReceiverRouter(sp.GetRequiredService<ActorSystem>()));
        }
        else
        {
            services.TryAddSingletonSource<IClusterMembershipSource>(_ =>
                new LocalClusterMembershipSource());

            services.TryAddSingletonSource<IJobReceiverRouter>(sp =>
                new LocalJobReceiverRouter(sp.GetRequiredService<ActorSystem>()));
        }

        services.TryAddSingletonSource<IJobExecutionPacer>(_ => new RandomJobExecutionPacer());
        services.TryAddSingletonSource<TimeProvider>(_ => TimeProvider.System);

        return services;
    }

    private static void TryAddSingletonSource<T>(
        this IServiceCollection services,
        Func<IServiceProvider, T> factory) where T : class
    {
        if (services.Any(descriptor => descriptor.ServiceType == typeof(T)))
            return;

        services.AddSingleton(factory);
    }

    /// <summary>
    /// Registers the whole domain: submitters, tracker, and this node's receiver(s).
    /// </summary>
    /// <param name="builder">The Akka.Hosting builder.</param>
    /// <param name="executionMode">Local or clustered infrastructure.</param>
    /// <param name="localNodes">
    /// <see cref="AkkaExecutionMode.LocalTest"/> only: the fake node host names to stand up
    /// receivers for. Ignored when clustered, where the single receiver uses the real self address.
    /// </param>
    public static AkkaConfigurationBuilder WithJobSchedulingActors(
        this AkkaConfigurationBuilder builder,
        AkkaExecutionMode executionMode = AkkaExecutionMode.LocalTest,
        IReadOnlyList<string>? localNodes = null) =>
        builder
            .WithJobSubmitters(executionMode)
            .WithJobTracker(executionMode)
            .WithJobReceivers(executionMode, localNodes);

    /// <summary>
    /// Registers the submitter entities under <see cref="JobSubmitterManagerKey"/>.
    /// </summary>
    public static AkkaConfigurationBuilder WithJobSubmitters(
        this AkkaConfigurationBuilder builder,
        AkkaExecutionMode executionMode = AkkaExecutionMode.LocalTest)
    {
        if (executionMode == AkkaExecutionMode.LocalTest)
        {
            return builder.WithActors((system, registry, _) =>
            {
                var parent = system.ActorOf(
                    GenericChildPerEntityParent.CreateProps(
                        new JobSubmitterMessageExtractor(),
                        entityId => SubmitterProps(registry, entityId)),
                    SubmitterRegionName);

                registry.Register<JobSubmitterManagerKey>(parent);
            });
        }

        // RememberEntities stays off: submitters are created on demand and hold nothing worth
        // resurrecting after a rebalance.
        return builder.WithShardRegion<JobSubmitterManagerKey>(
            SubmitterRegionName,
            (_, registry, _) => entityId => SubmitterProps(registry, entityId),
            new JobSubmitterMessageExtractor(),
            new ShardOptions
            {
                StateStoreMode = StateStoreMode.DData,
                RememberEntities = false,
                Role = WorkerRole
            });
    }

    /// <summary>
    /// Registers the job tracker under <see cref="JobTrackerKey"/> — the actor itself locally, a
    /// cluster singleton proxy when clustered.
    /// </summary>
    public static AkkaConfigurationBuilder WithJobTracker(
        this AkkaConfigurationBuilder builder,
        AkkaExecutionMode executionMode = AkkaExecutionMode.LocalTest)
    {
        if (executionMode == AkkaExecutionMode.LocalTest)
        {
            return builder.WithActors((system, registry, resolver) =>
            {
                var tracker = system.ActorOf(
                    resolver.Props<JobTrackerActor>(),
                    JobTrackerActor.Name);

                registry.Register<JobTrackerKey>(tracker);
            });
        }

        return builder.WithSingleton<JobTrackerKey>(
            JobTrackerActor.Name,
            (_, _, resolver) => resolver.Props<JobTrackerActor>(),
            new ClusterSingletonOptions { Role = WorkerRole },
            createProxyToo: true);
    }

    /// <summary>
    /// Registers this node's receiver — or, locally, one per fake node.
    /// </summary>
    public static AkkaConfigurationBuilder WithJobReceivers(
        this AkkaConfigurationBuilder builder,
        AkkaExecutionMode executionMode = AkkaExecutionMode.LocalTest,
        IReadOnlyList<string>? localNodes = null)
    {
        if (executionMode == AkkaExecutionMode.LocalTest)
        {
            var hosts = localNodes ?? [];

            return builder.WithActors((system, registry, resolver) =>
            {
                // Reproduce the remote layout locally — /user/nodes/{host}/job-receiver — which is
                // exactly what LocalJobReceiverRouter resolves against.
                var nodes = system.ActorOf(
                    LocalNodesActor.Props(hosts, registry.Get<JobTrackerKey>(), Pacer(resolver)),
                    LocalNodesName);

                registry.Register<JobReceiverKey>(nodes);
            });
        }

        return builder.WithActors((system, registry, resolver) =>
        {
            var selfAddress = Akka.Cluster.Cluster.Get(system).SelfAddress;

            var receiver = system.ActorOf(
                JobReceiverActor.Props(selfAddress, registry.Get<JobTrackerKey>(), Pacer(resolver)),
                JobReceiverActor.Name);

            registry.Register<JobReceiverKey>(receiver);
        });
    }

    private static Props SubmitterProps(IActorRegistry registry, string entityId) =>
        JobSubmitterActor.Props(new JobSubmitterId(entityId), registry.Get<JobTrackerKey>());

    private static IJobExecutionPacer Pacer(IDependencyResolver resolver) =>
        resolver.GetService<IJobExecutionPacer>();
}
