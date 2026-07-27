using Akka.Actor;
using Akka.CustomJobScheduling.Core.Actors;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.Hosting;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// Akka.Hosting registration for <see cref="JobSchedulingSerializer"/>.
/// </summary>
public static class SerializationHostingExtensions
{
    /// <summary>
    /// Binds the job scheduling domain to its MessagePack serializer.
    /// </summary>
    /// <remarks>
    /// Bindings are declared against the marker interfaces, so a new event or command is covered
    /// the moment it implements one — there is no per-type list to keep in sync here. What does
    /// need keeping in sync is the manifest switch inside the serializer, which fails loudly on an
    /// unknown type rather than silently falling back to JSON.
    /// </remarks>
    public static AkkaConfigurationBuilder AddJobSchedulingSerializer(
        this AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(
            serializerIdentifier: "job-scheduling",
            boundTypes:
            [
                typeof(IJobTrackerDomain),
                typeof(ExecutionMessages.ExecuteJob),
                typeof(ExecutionMessages.CancelExecution)
            ],
            serializerFactory: system => new JobSchedulingSerializer((ExtendedActorSystem)system));
}
