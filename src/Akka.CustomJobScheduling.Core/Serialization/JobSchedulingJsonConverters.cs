using System.Text.Json;
using System.Text.Json.Serialization;
using Akka.Actor;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// Renders <see cref="JobId"/> as a bare JSON string, and as an object key.
/// </summary>
/// <remarks>
/// The property-name overrides matter because <see cref="JobTrackerState.Jobs"/> is keyed by
/// <see cref="JobId"/> — without them System.Text.Json can't use it as a dictionary key.
/// </remarks>
public sealed class JobIdJsonConverter : JsonConverter<JobId>
{
    public override JobId Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        new(reader.GetString() ?? string.Empty);

    public override void Write(Utf8JsonWriter writer, JobId value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.Value);

    public override JobId ReadAsPropertyName(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        new(reader.GetString() ?? string.Empty);

    public override void WriteAsPropertyName(Utf8JsonWriter writer, JobId value, JsonSerializerOptions options) =>
        writer.WritePropertyName(value.Value);
}

/// <summary>Renders <see cref="JobSubmitterId"/> as a bare JSON string.</summary>
public sealed class JobSubmitterIdJsonConverter : JsonConverter<JobSubmitterId>
{
    public override JobSubmitterId Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        new(reader.GetString() ?? string.Empty);

    public override void Write(Utf8JsonWriter writer, JobSubmitterId value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.Value);
}

/// <summary>Renders <see cref="JobSize"/> as a bare JSON number rather than <c>{"size":N}</c>.</summary>
public sealed class JobSizeJsonConverter : JsonConverter<JobSize>
{
    public override JobSize Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        new(reader.GetUInt32());

    public override void Write(Utf8JsonWriter writer, JobSize value, JsonSerializerOptions options) =>
        writer.WriteNumberValue(value.Size);
}

/// <summary>
/// Renders an <see cref="Address"/> as its canonical string, and as an object key.
/// </summary>
/// <remarks>
/// <see cref="JobTrackerState.Nodes"/> is keyed by <see cref="Address"/>, so the property-name
/// overrides are required for that dictionary to serialize.
/// </remarks>
public sealed class AddressJsonConverter : JsonConverter<Address>
{
    public override Address Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        Address.Parse(reader.GetString() ?? string.Empty);

    public override void Write(Utf8JsonWriter writer, Address value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.ToString());

    public override Address ReadAsPropertyName(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
        Address.Parse(reader.GetString() ?? string.Empty);

    public override void WriteAsPropertyName(Utf8JsonWriter writer, Address value, JsonSerializerOptions options) =>
        writer.WritePropertyName(value.ToString());
}

/// <summary>
/// Renders an <see cref="IActorRef"/> as its serialized actor path.
/// </summary>
/// <remarks>
/// Unlike the other converters this one is stateful: resolving a path back to a usable reference
/// needs the actor system, which is why the serializer builds its options with a fresh instance
/// rather than sharing a static one. The path carries the full remote address, so a subscription
/// sent from another node round-trips to something the tracker can actually reply to.
/// </remarks>
public sealed class ActorRefJsonConverter : JsonConverter<IActorRef>
{
    private readonly ExtendedActorSystem _system;

    public ActorRefJsonConverter(ExtendedActorSystem system) => _system = system;

    public override IActorRef Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options)
    {
        var path = reader.GetString();
        return path is null ? ActorRefs.Nobody : _system.Provider.ResolveActorRef(path);
    }

    public override void Write(Utf8JsonWriter writer, IActorRef value, JsonSerializerOptions options) =>
        writer.WriteStringValue(Akka.Serialization.Serialization.SerializedActorPath(value));
}
