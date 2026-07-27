using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using Akka.Serialization;
using MessagePack;

namespace Akka.CustomJobScheduling.Core.Serialization;

/// <summary>
/// Read/write helpers for the domain's value objects.
/// </summary>
/// <remarks>
/// <para>
/// Every composite is framed as a MessagePack array, and readers tolerate a longer array than they
/// know how to read. That gives one evolution rule, which is the only rule anyone has to remember:
/// <b>append new fields at the end, never reorder or remove.</b> An old process then skips fields
/// it doesn't understand instead of failing, and a new process reading old data sees a short array
/// and fills in defaults.
/// </para>
/// <para>
/// Written by hand rather than by a reflection-based resolver on purpose: the domain records stay
/// free of serialization attributes, and the wire schema is something you can read.
/// </para>
/// </remarks>
internal static class WireFormat
{
    // ---- scalars ----

    public static void Write(ref MessagePackWriter w, JobId value) => w.Write(value.Value);

    public static JobId ReadJobId(ref MessagePackReader r) => new(r.ReadString() ?? string.Empty);

    public static void Write(ref MessagePackWriter w, JobSubmitterId value) => w.Write(value.Value);

    public static JobSubmitterId ReadSubmitterId(ref MessagePackReader r) =>
        new(r.ReadString() ?? string.Empty);

    public static void Write(ref MessagePackWriter w, JobSize value) => w.Write(value.Size);

    public static JobSize ReadJobSize(ref MessagePackReader r) => new(r.ReadUInt32());

    /// <summary>
    /// Timestamps normalise to UTC ticks. The original offset is not preserved — these are instants,
    /// and a journal that recorded local offsets would replay differently depending on where it ran.
    /// </summary>
    public static void Write(ref MessagePackWriter w, DateTimeOffset value) => w.Write(value.UtcTicks);

    public static DateTimeOffset ReadTimestamp(ref MessagePackReader r) =>
        new(r.ReadInt64(), TimeSpan.Zero);

    public static void Write(ref MessagePackWriter w, DateTimeOffset? value)
    {
        if (value is null)
        {
            w.WriteNil();
            return;
        }

        Write(ref w, value.Value);
    }

    public static DateTimeOffset? ReadTimestampOrNull(ref MessagePackReader r)
    {
        if (r.TryReadNil())
            return null;

        return ReadTimestamp(ref r);
    }

    public static void Write(ref MessagePackWriter w, Address? value) => w.Write(value?.ToString());

    public static Address? ReadAddressOrNull(ref MessagePackReader r)
    {
        var text = r.ReadString();
        return text is null ? null : Address.Parse(text);
    }

    public static Address ReadAddress(ref MessagePackReader r) =>
        ReadAddressOrNull(ref r) ?? Address.AllSystems;

    public static void Write(ref MessagePackWriter w, MemberStatus value) => w.Write((int)value);

    public static MemberStatus ReadMemberStatus(ref MessagePackReader r) =>
        (MemberStatus)r.ReadInt32();

    public static void Write(ref MessagePackWriter w, JobStatus value) => w.Write((int)value);

    public static JobStatus ReadJobStatus(ref MessagePackReader r) => (JobStatus)r.ReadInt32();

    /// <summary>
    /// Actor references travel as Akka's serialized actor path, which carries the full remote
    /// address so the receiving node can resolve it back to something it can send to.
    /// </summary>
    public static void Write(ref MessagePackWriter w, IActorRef value) =>
        w.Write(Akka.Serialization.Serialization.SerializedActorPath(value));

    public static IActorRef ReadActorRef(ref MessagePackReader r, ExtendedActorSystem system)
    {
        var path = r.ReadString();
        return path is null ? ActorRefs.Nobody : system.Provider.ResolveActorRef(path);
    }

    // ---- composites ----

    public static void Write(ref MessagePackWriter w, WorkProgress value)
    {
        w.WriteArrayHeader(2);
        Write(ref w, value.Completed);
        Write(ref w, value.Total);
    }

    public static WorkProgress ReadWorkProgress(ref MessagePackReader r)
    {
        var fields = r.ReadArrayHeader();
        var completed = ReadJobSize(ref r);
        var total = ReadJobSize(ref r);
        Skip(ref r, fields, 2);

        return new WorkProgress(completed, total);
    }

    public static void Write(ref MessagePackWriter w, JobDefinition value)
    {
        w.WriteArrayHeader(2);
        Write(ref w, value.Id);
        Write(ref w, value.Size);
    }

    public static JobDefinition ReadJobDefinition(ref MessagePackReader r)
    {
        var fields = r.ReadArrayHeader();
        var id = ReadJobId(ref r);
        var size = ReadJobSize(ref r);
        Skip(ref r, fields, 2);

        return new JobDefinition(id, size);
    }

    public static void Write(ref MessagePackWriter w, JobProgress value)
    {
        w.WriteArrayHeader(4);
        Write(ref w, value.Id);
        Write(ref w, value.Status);
        Write(ref w, value.Progress);
        Write(ref w, value.LastUpdatedAt);
    }

    public static JobProgress ReadJobProgress(ref MessagePackReader r)
    {
        var fields = r.ReadArrayHeader();
        var id = ReadJobId(ref r);
        var status = ReadJobStatus(ref r);
        var progress = ReadWorkProgress(ref r);
        var updatedAt = ReadTimestamp(ref r);
        Skip(ref r, fields, 4);

        return new JobProgress(id, status, progress, updatedAt);
    }

    public static void Write(ref MessagePackWriter w, NodeStatus value)
    {
        w.WriteArrayHeader(6);
        Write(ref w, value.NodeAddress);
        Write(ref w, value.Status);
        w.Write(value.Reachable);
        Write(ref w, value.LastUpdatedAt);
        Write(ref w, value.MaximumCapacity);
        Write(ref w, value.CapacityInUse);
    }

    public static NodeStatus ReadNodeStatus(ref MessagePackReader r)
    {
        var fields = r.ReadArrayHeader();
        var address = ReadAddress(ref r);
        var status = ReadMemberStatus(ref r);
        var reachable = r.ReadBoolean();
        var updatedAt = ReadTimestamp(ref r);
        var maximum = ReadJobSize(ref r);
        var inUse = ReadJobSize(ref r);
        Skip(ref r, fields, 6);

        return new NodeStatus(address, status, reachable, updatedAt, maximum, inUse);
    }

    public static void Write(ref MessagePackWriter w, TrackedJob value)
    {
        w.WriteArrayHeader(8);
        Write(ref w, value.Definition);
        Write(ref w, value.SubmitterId);
        Write(ref w, value.Status);
        Write(ref w, value.Progress);
        Write(ref w, value.AssignedNode);
        Write(ref w, value.LastUpdatedAt);
        Write(ref w, value.SubmittedAt);
        Write(ref w, value.StartedAt);
    }

    public static TrackedJob ReadTrackedJob(ref MessagePackReader r)
    {
        var fields = r.ReadArrayHeader();
        var definition = ReadJobDefinition(ref r);
        var submitter = ReadSubmitterId(ref r);
        var status = ReadJobStatus(ref r);
        var progress = ReadWorkProgress(ref r);
        var assigned = ReadAddressOrNull(ref r);
        var updatedAt = ReadTimestamp(ref r);

        // SubmittedAt and StartedAt were appended after the first release, so snapshots already in
        // the journal stop at six fields. Last-updated is the closest thing those records carry.
        var submittedAt = updatedAt;
        if (fields > 6)
            submittedAt = ReadTimestamp(ref r);

        DateTimeOffset? startedAt = null;
        if (fields > 7)
            startedAt = ReadTimestampOrNull(ref r);

        Skip(ref r, fields, 8);

        return new TrackedJob(
            definition, submitter, status, progress, assigned, updatedAt, submittedAt, startedAt);
    }

    /// <summary>
    /// Discards fields written by a newer version of the schema.
    /// </summary>
    public static void Skip(ref MessagePackReader r, int written, int consumed)
    {
        for (var i = consumed; i < written; i++)
        {
            r.Skip();
        }
    }
}
