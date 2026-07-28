using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.CustomJobScheduling.Core.Actors.Cluster;
using Akka.CustomJobScheduling.Core.Actors.Streaming;
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
                    new JobSize(nodeCapacity),
                    WorkerRole));

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

    /// <summary>
    /// Registers everything a node needs to <i>use</i> the scheduler without hosting any of it:
    /// proxies to the tracker singleton and the submitter shard region, plus the local feed
    /// supervisor. This is what an API node wants.
    /// </summary>
    /// <remarks>
    /// In <see cref="AkkaExecutionMode.LocalTest"/> there is nothing to proxy to, so this registers
    /// the real thing — which is what makes an API integration test runnable in one process.
    /// </remarks>
    public static AkkaConfigurationBuilder WithJobSchedulingClient(
        this AkkaConfigurationBuilder builder,
        AkkaExecutionMode executionMode = AkkaExecutionMode.LocalTest)
    {
        if (executionMode == AkkaExecutionMode.LocalTest)
        {
            return builder
                .WithJobSubmitters(executionMode)
                .WithJobTracker(executionMode)
                .WithJobStreams();
        }

        return builder
            .WithSingletonProxy<JobTrackerKey>(
                JobTrackerActor.Name,
                new ClusterSingletonOptions { Role = WorkerRole })
            .WithShardRegionProxy<JobSubmitterManagerKey>(
                SubmitterRegionName,
                WorkerRole,
                new JobSubmitterMessageExtractor())
            .WithJobStreams();
    }

    /// <summary>
    /// Announces the local stand-in nodes to the membership source, so a self-contained local host
    /// has capacity without anything having to drive topology by hand.
    /// </summary>
    /// <remarks>
    /// Deliberately separate from <see cref="WithJobReceivers"/>. Tests that exercise topology
    /// transitions register their own <see cref="LocalClusterMembershipSource"/> and call
    /// <c>MemberUp</c> themselves; auto-announcing there would hand them nodes they never asked
    /// for. Everything else — a locally-run API, a demo — wants the nodes to just exist.
    /// </remarks>
    public static AkkaConfigurationBuilder WithAnnouncedLocalNodes(
        this AkkaConfigurationBuilder builder,
        IReadOnlyList<string> hosts,
        uint nodeCapacity = DefaultNodeCapacity) =>
        builder.WithActors((_, _, resolver) =>
        {
            if (resolver.GetService<IClusterMembershipSource>() is not LocalClusterMembershipSource local)
                return;

            foreach (var host in hosts)
            {
                local.MemberUp(host, nodeCapacity);
            }
        });

    /// <summary>
    /// Registers the node-local supervisor that owns live job feeds. No execution-mode overload:
    /// feeds are always local to the node terminating the connection.
    /// </summary>
    public static AkkaConfigurationBuilder WithJobStreams(this AkkaConfigurationBuilder builder) =>
        builder.WithActors((system, registry, resolver) =>
        {
            var supervisor = system.ActorOf(
                resolver.Props<JobStreamSupervisor>(),
                JobStreamSupervisor.Name);

            registry.Register<JobStreamSupervisorKey>(supervisor);
        });

    private static Props SubmitterProps(IActorRegistry registry, string entityId) =>
        // The entity id is URI-encoded by JobSubmitterMessageExtractor so it is a valid actor name;
        // decode it back to the submitter's real id here.
        JobSubmitterActor.Props(
            new JobSubmitterId(Uri.UnescapeDataString(entityId)),
            registry.Get<JobTrackerKey>());

    private static IJobExecutionPacer Pacer(IDependencyResolver resolver) =>
        resolver.GetService<IJobExecutionPacer>();
}
