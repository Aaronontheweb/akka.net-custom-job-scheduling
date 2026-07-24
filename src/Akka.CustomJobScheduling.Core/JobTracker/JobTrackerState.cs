using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// The outcome of handing an <see cref="IJobTrackerCommand"/> to <see cref="JobTrackerState.Decide"/>:
/// the facts to persist, and what to tell the sender.
/// </summary>
/// <param name="Events">Events to persist and then fold back into the state, in order.</param>
/// <param name="Response">
/// The acknowledgement to send back, or <c>null</c> for internal commands nobody is waiting on.
/// </param>
public sealed record JobTrackerDecision(
    ImmutableArray<IJobTrackerEvent> Events,
    IJobTrackerCommandResponse? Response)
{
    public static readonly JobTrackerDecision None =
        new(ImmutableArray<IJobTrackerEvent>.Empty, null);

    public bool WasRejected => Response is JobTrackerResponses.CommandRejected;

    /// <summary>Command validated; here are its effects.</summary>
    public static JobTrackerDecision Accept(JobId id, params IJobTrackerEvent[] events) =>
        new([.. events], new JobTrackerResponses.CommandAccepted(id));

    /// <summary>Command refused; nothing changes.</summary>
    public static JobTrackerDecision Reject(JobId id, JobRejectionReason reason, string message) =>
        new(
            ImmutableArray<IJobTrackerEvent>.Empty,
            new JobTrackerResponses.CommandRejected(id, reason, message));

    /// <summary>Effects with no acknowledgement — used for cluster topology changes.</summary>
    public static JobTrackerDecision Record(params IJobTrackerEvent[] events) =>
        new([.. events], null);

    public JobTrackerDecision Then(ImmutableArray<IJobTrackerEvent> more)
    {
        if (more.IsEmpty)
            return this;

        return this with { Events = Events.AddRange(more) };
    }
}

/// <summary>
/// The complete durable state of the job tracker: a queue of jobs awaiting placement, every job we
/// know about, and a map of all available nodes.
/// </summary>
/// <remarks>
/// <para>
/// Nothing in here is actor-shaped. No <see cref="IActorRef"/>, no <c>Context</c>, no
/// <c>ActorSystem</c>, and no call to <c>DateTimeOffset.UtcNow</c>. Time arrives through the
/// <c>now</c> parameter on <see cref="Decide"/> and is then carried by the events themselves, which
/// is what makes recovery deterministic: replaying a journal always produces the same state, no
/// matter when you replay it.
/// </para>
/// <para>The split is:</para>
/// <list type="bullet">
/// <item><description>
/// <see cref="Decide"/> — validates a command and returns the events it implies. Pure.
/// </description></item>
/// <item><description>
/// <see cref="Apply"/> — folds one event into a new state. Never validates; an event is a fact.
/// </description></item>
/// <item><description>
/// The tracker <i>actor</i> — reads the clock, persists what <see cref="Decide"/> returned, folds it,
/// replies to <c>Sender</c>, notifies subscribers. That's all it does.
/// </description></item>
/// </list>
/// <para>
/// Record equality is not meaningful here — the generated <c>Equals</c> falls back to reference
/// comparison for the immutable collections. Compare the members instead.
/// </para>
/// </remarks>
public sealed record JobTrackerState : IJobTrackerDomain
{
    public static readonly JobTrackerState Empty = new();

    /// <summary>Jobs accepted but not yet placed, in the order they'll be considered.</summary>
    public ImmutableList<JobId> PendingJobs { get; init; } = ImmutableList<JobId>.Empty;

    /// <summary>Every job the tracker knows about, including finished ones.</summary>
    public ImmutableDictionary<JobId, TrackedJob> Jobs { get; init; } =
        ImmutableDictionary<JobId, TrackedJob>.Empty;

    /// <summary>All nodes currently part of the cluster.</summary>
    public ImmutableDictionary<Address, NodeStatus> Nodes { get; init; } =
        ImmutableDictionary<Address, NodeStatus>.Empty;

    // ------------------------------------------------------------------
    // Decide: command -> events
    // ------------------------------------------------------------------

    /// <summary>
    /// Validates <paramref name="command"/> against the current state and returns the events it
    /// implies. Does not modify this instance.
    /// </summary>
    /// <param name="now">
    /// The timestamp to stamp onto resulting events. Supplied by the caller so transitions stay
    /// deterministic and testable.
    /// </param>
    /// <remarks>
    /// Any command that succeeds also drains the queue, so placement lives in one place instead of
    /// being scattered across the command handlers.
    /// </remarks>
    public JobTrackerDecision Decide(IJobTrackerCommand command, DateTimeOffset now)
    {
        var decision = Evaluate(command, now);

        if (decision.WasRejected)
            return decision;

        var placements = Fold(decision.Events).PlacePendingJobs(now);

        return decision.Then(placements);
    }

    private JobTrackerDecision Evaluate(IJobTrackerCommand command, DateTimeOffset now) =>
        command switch
        {
            JobTrackerCommands.SubmitJob c => OnSubmitJob(c, now),
            JobTrackerCommands.CancelJob c => OnCancelJob(c, now),
            JobTrackerCommands.ReportProgress c => OnReportProgress(c, now),
            JobTrackerCommands.ReportJobCompleted c => OnReportJobCompleted(c, now),
            JobTrackerCommands.ReportJobFailed c => OnReportJobFailed(c, now),
            JobTrackerCommands.NodeJoined c => OnNodeJoined(c, now),
            JobTrackerCommands.NodeLeft c => OnNodeLeft(c, now),
            JobTrackerCommands.NodeReachabilityChanged c => OnNodeReachabilityChanged(c, now),
            JobTrackerCommands.SyncNodes c => OnSyncNodes(c, now),

            // Drain is handled by Decide itself — the tick exists only to trigger it.
            _ => JobTrackerDecision.None
        };

    private JobTrackerDecision OnSubmitJob(JobTrackerCommands.SubmitJob command, DateTimeOffset now)
    {
        if (Jobs.ContainsKey(command.Id))
            return Rejected(command.Id, JobRejectionReason.DuplicateJobId, "already submitted");

        if (IsTooLargeForCluster(command.Job))
            return Rejected(
                command.Id,
                JobRejectionReason.ExceedsClusterCapacity,
                $"needs {command.Job.Size.Size} units; no node has that much total capacity");

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobAccepted(command.Job, command.SubmitterId, now));
    }

    /// <summary>
    /// An empty cluster is not a rejection — nodes may still be joining. But if we know of nodes and
    /// none is big enough, the job would queue forever, so fail fast instead.
    /// </summary>
    private bool IsTooLargeForCluster(JobDefinition job)
    {
        if (Nodes.IsEmpty)
            return false;

        return Nodes.Values.All(node => node.MaximumCapacity < job.Size);
    }

    private JobTrackerDecision OnCancelJob(JobTrackerCommands.CancelJob command, DateTimeOffset now)
    {
        if (!Jobs.TryGetValue(command.Id, out var job))
            return UnknownJob(command.Id);

        if (job.SubmitterId != command.SubmitterId)
            return Rejected(
                command.Id,
                JobRejectionReason.NotSubmitter,
                "submitted by someone else");

        if (job.IsTerminal)
            return Rejected(
                command.Id,
                JobRejectionReason.AlreadyTerminal,
                $"already {job.Status}");

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobCancelled(command.Id, command.Reason, now));
    }

    private JobTrackerDecision OnReportProgress(
        JobTrackerCommands.ReportProgress command,
        DateTimeOffset now)
    {
        var rejection = ValidateReport(command.Id, command.NodeAddress);
        if (rejection is not null)
            return rejection;

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobProgressed(command.Id, command.Progress, now));
    }

    private JobTrackerDecision OnReportJobCompleted(
        JobTrackerCommands.ReportJobCompleted command,
        DateTimeOffset now)
    {
        var rejection = ValidateReport(command.Id, command.NodeAddress);
        if (rejection is not null)
            return rejection;

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobCompleted(command.Id, now));
    }

    private JobTrackerDecision OnReportJobFailed(
        JobTrackerCommands.ReportJobFailed command,
        DateTimeOffset now)
    {
        var rejection = ValidateReport(command.Id, command.NodeAddress);
        if (rejection is not null)
            return rejection;

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobFailed(command.Id, command.Reason, now));
    }

    /// <summary>
    /// Shared guard for worker reports: returns the rejection, or <c>null</c> if the report is good.
    /// </summary>
    private JobTrackerDecision? ValidateReport(JobId id, Address from)
    {
        if (!Jobs.TryGetValue(id, out var job))
            return UnknownJob(id);

        if (!job.IsRunningOn(from))
            return Rejected(id, JobRejectionReason.StaleReport, $"not running on {from}");

        return null;
    }

    private JobTrackerDecision OnNodeJoined(
        JobTrackerCommands.NodeJoined command,
        DateTimeOffset now)
    {
        if (!Nodes.TryGetValue(command.NodeAddress, out var existing))
            return JobTrackerDecision.Record(
                new JobTrackerEvents.NodeAdded(
                    command.NodeAddress,
                    command.Status,
                    command.MaxCapacity,
                    now));

        if (existing.Status == command.Status && existing.Reachable)
            return JobTrackerDecision.None;

        // Already known — this is a membership transition (e.g. WeaklyUp -> Up), not a new node.
        // Treat it as a status change so we don't discard the capacity it's already committed to.
        return JobTrackerDecision.Record(
            new JobTrackerEvents.NodeStatusChanged(
                command.NodeAddress,
                command.Status,
                Reachable: true,
                now));
    }

    private JobTrackerDecision OnNodeLeft(JobTrackerCommands.NodeLeft command, DateTimeOffset now)
    {
        if (!Nodes.ContainsKey(command.NodeAddress))
            return JobTrackerDecision.None;

        // Requeue before forgetting the node, so capacity accounting unwinds in the right order.
        var events = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        foreach (var job in JobsRunningOn(command.NodeAddress))
        {
            events.Add(new JobTrackerEvents.JobRequeued(
                job.Id,
                command.NodeAddress,
                "Node left the cluster.",
                now));
        }

        events.Add(new JobTrackerEvents.NodeRemoved(command.NodeAddress, now));

        return JobTrackerDecision.Record([.. events]);
    }

    private JobTrackerDecision OnNodeReachabilityChanged(
        JobTrackerCommands.NodeReachabilityChanged command,
        DateTimeOffset now)
    {
        if (!Nodes.TryGetValue(command.NodeAddress, out var existing))
            return JobTrackerDecision.None;

        if (existing.Reachable == command.Reachable)
            return JobTrackerDecision.None;

        /*
         * Unreachable nodes stop receiving new work but keep the jobs they already hold. We do NOT
         * requeue here, and that's deliberate.
         *
         * Unreachability is a transient verdict from Akka.Cluster's failure detector, not a
         * decision. It resolves one of two ways:
         *
         *   1. The partition heals. Akka.Cluster emits ReachableMember, we get
         *      NodeReachabilityChanged(true), and the node resumes taking work with its running
         *      jobs untouched.
         *
         *   2. It doesn't heal. The downing provider — the split brain resolver, which Akka.NET
         *      enables by default in 1.5 — marks the node Down once it has been unreachable for
         *      `stable-after`. Akka.Cluster then emits MemberRemoved, the tracker actor turns that
         *      into NodeLeft, and OnNodeLeft requeues every job the node was holding.
         *
         * So the jobs do get requeued on a genuinely dead node; it just happens on removal rather
         * than on unreachability. Waiting for that is the whole point: from this side of a network
         * partition we cannot tell "the node is gone" from "we can't currently see the node." A
         * node on the far side of a healing partition is still happily running its jobs, so
         * requeueing on unreachability alone would place the same job twice and let both copies run
         * to completion. Waiting for MemberRemoved keeps us at one live placement per job, because
         * a downed node is guaranteed to be shutting down rather than continuing to work.
         *
         * The cost is latency: a job on an unreachable node stalls for `stable-after` before it
         * gets rescheduled. That's a knob on the downing provider, not on this state machine.
         */
        return JobTrackerDecision.Record(
            new JobTrackerEvents.NodeStatusChanged(
                command.NodeAddress,
                existing.Status,
                command.Reachable,
                now));
    }

    /// <summary>
    /// Reconciles the whole node map against the authoritative membership in one step.
    /// </summary>
    /// <remarks>
    /// Needed because the tracker is persistent. Replaying the journal faithfully restores nodes
    /// that were present when the events were written; if one of them left while the tracker was
    /// down, no <see cref="JobTrackerCommands.NodeLeft"/> is ever coming for it, and its jobs would
    /// stay <see cref="JobStatus.Running"/> on a node that isn't there. Removals are emitted before
    /// additions so freed capacity is available to the placement pass that follows.
    /// </remarks>
    private JobTrackerDecision OnSyncNodes(JobTrackerCommands.SyncNodes command, DateTimeOffset now)
    {
        var events = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        var departed = Nodes.Keys
            .Where(address => !command.Members.ContainsKey(address))
            .OrderBy(address => address.ToString(), StringComparer.Ordinal);

        foreach (var address in departed)
        {
            foreach (var job in JobsRunningOn(address))
            {
                events.Add(new JobTrackerEvents.JobRequeued(
                    job.Id,
                    address,
                    "Node is no longer a cluster member.",
                    now));
            }

            events.Add(new JobTrackerEvents.NodeRemoved(address, now));
        }

        var arrived = command.Members
            .Where(member => !Nodes.ContainsKey(member.Key))
            .OrderBy(member => member.Key.ToString(), StringComparer.Ordinal);

        foreach (var (address, capacity) in arrived)
        {
            events.Add(new JobTrackerEvents.NodeAdded(address, MemberStatus.Up, capacity, now));
        }

        return events.Count == 0
            ? JobTrackerDecision.None
            : JobTrackerDecision.Record([.. events]);
    }

    private static JobTrackerDecision UnknownJob(JobId id) =>
        Rejected(id, JobRejectionReason.UnknownJob, "not known to this tracker");

    private static JobTrackerDecision Rejected(JobId id, JobRejectionReason reason, string why) =>
        JobTrackerDecision.Reject(id, reason, $"Job '{id.Value}' {why}.");

    // ------------------------------------------------------------------
    // Placement
    // ------------------------------------------------------------------

    /// <summary>
    /// Walks the pending queue in order, emitting a <see cref="JobTrackerEvents.JobScheduled"/> for
    /// each job that fits on an eligible node.
    /// </summary>
    /// <remarks>
    /// Strict FIFO with head-of-line blocking: if the job at the front doesn't fit anywhere we stop
    /// rather than skipping ahead to smaller jobs. That trades some utilization for the guarantee
    /// that a large job can't be starved by a stream of small ones. This is the policy seam.
    /// </remarks>
    public ImmutableArray<IJobTrackerEvent> PlacePendingJobs(DateTimeOffset now)
    {
        if (PendingJobs.IsEmpty || Nodes.IsEmpty)
            return ImmutableArray<IJobTrackerEvent>.Empty;

        var projected = this;
        var placements = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        while (!projected.PendingJobs.IsEmpty)
        {
            var nextId = projected.PendingJobs[0];

            // Defensive: Apply keeps PendingJobs and Jobs in lockstep, so this shouldn't happen.
            // Drop the orphan locally rather than spinning forever on it.
            if (!projected.Jobs.TryGetValue(nextId, out var job))
            {
                projected = projected.Dequeue(nextId);
                continue;
            }

            var target = projected.SelectNodeFor(job.Definition);
            if (target is null)
                break;

            var scheduled = new JobTrackerEvents.JobScheduled(nextId, target, now);
            placements.Add(scheduled);
            projected = projected.Apply(scheduled);
        }

        return placements.ToImmutable();
    }

    /// <summary>
    /// The eligible node with the most spare capacity, ties broken deterministically by address.
    /// </summary>
    private Address? SelectNodeFor(JobDefinition job) =>
        Nodes.Values
            .Where(node => node.CanAccept(job))
            .OrderBy(node => node, MostAvailableCapacityComparer.Instance)
            .Select(node => node.NodeAddress)
            .FirstOrDefault();

    // ------------------------------------------------------------------
    // Apply: event -> new state
    // ------------------------------------------------------------------

    /// <summary>
    /// Folds one event into a new state. This is the only thing that needs to run during recovery.
    /// </summary>
    public JobTrackerState Apply(IJobTrackerEvent @event) => @event switch
    {
        JobTrackerEvents.NodeAdded e => ApplyNodeAdded(e),
        JobTrackerEvents.NodeRemoved e => ForgetNode(e.NodeAddress),
        JobTrackerEvents.NodeStatusChanged e => ApplyNodeStatusChanged(e),
        JobTrackerEvents.JobAccepted e => ApplyJobAccepted(e),
        JobTrackerEvents.JobScheduled e => ApplyJobScheduled(e),
        JobTrackerEvents.JobProgressed e => ApplyJobProgressed(e),
        JobTrackerEvents.JobCompleted e => ApplyJobCompleted(e),
        JobTrackerEvents.JobFailed e => ApplyJobFailed(e),
        JobTrackerEvents.JobCancelled e => ApplyJobCancelled(e),
        JobTrackerEvents.JobRequeued e => ApplyJobRequeued(e),
        _ => this
    };

    /// <summary>Folds a sequence of events in order — the recovery entry point.</summary>
    public JobTrackerState Fold(IEnumerable<IJobTrackerEvent> events) =>
        events.Aggregate(this, (state, @event) => state.Apply(@event));

    private JobTrackerState ApplyNodeAdded(JobTrackerEvents.NodeAdded e) =>
        WithNode(NodeStatus.Joined(e.NodeAddress, e.Status, e.MaxCapacity, e.OccurredAt));

    private JobTrackerState ApplyNodeStatusChanged(JobTrackerEvents.NodeStatusChanged e)
    {
        if (!Nodes.TryGetValue(e.NodeAddress, out var node))
            return this;

        return WithNode(node.WithStatus(e.Status, e.Reachable, e.OccurredAt));
    }

    private JobTrackerState ApplyJobAccepted(JobTrackerEvents.JobAccepted e)
    {
        var job = TrackedJob.Waiting(e.Job, e.SubmitterId, e.OccurredAt);

        return WithJob(job).Enqueue(job.Id);
    }

    private JobTrackerState ApplyJobScheduled(JobTrackerEvents.JobScheduled e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return WithJob(job.RunOn(e.NodeAddress, e.OccurredAt))
            .Dequeue(e.Id)
            .Reserve(e.NodeAddress, job.Size);
    }

    private JobTrackerState ApplyJobProgressed(JobTrackerEvents.JobProgressed e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return WithJob(job.WithProgress(e.Progress, e.OccurredAt));
    }

    private JobTrackerState ApplyJobCompleted(JobTrackerEvents.JobCompleted e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return Finish(job, job.Complete(e.OccurredAt));
    }

    private JobTrackerState ApplyJobFailed(JobTrackerEvents.JobFailed e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return Finish(job, job.Fail(e.OccurredAt));
    }

    private JobTrackerState ApplyJobCancelled(JobTrackerEvents.JobCancelled e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return Finish(job, job.Cancel(e.OccurredAt));
    }

    private JobTrackerState ApplyJobRequeued(JobTrackerEvents.JobRequeued e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        return WithJob(job.Requeue(e.OccurredAt))
            .EnqueueFront(e.Id)
            .ReleaseFrom(e.PreviousNode, job.Size);
    }

    /// <summary>
    /// Common tail for every terminal transition: record it, drop it from the queue (in case it was
    /// never placed), and hand its capacity back.
    /// </summary>
    private JobTrackerState Finish(TrackedJob before, TrackedJob after) =>
        WithJob(after)
            .Dequeue(before.Id)
            .ReleaseFrom(before.AssignedNode, before.Size);

    // ------------------------------------------------------------------
    // Single-purpose state updates
    // ------------------------------------------------------------------

    private JobTrackerState WithJob(TrackedJob job) =>
        this with { Jobs = Jobs.SetItem(job.Id, job) };

    private JobTrackerState WithNode(NodeStatus node) =>
        this with { Nodes = Nodes.SetItem(node.NodeAddress, node) };

    private JobTrackerState ForgetNode(Address address) =>
        this with { Nodes = Nodes.Remove(address) };

    private JobTrackerState Enqueue(JobId id) =>
        this with { PendingJobs = PendingJobs.Add(id) };

    /// <summary>Back to the front of the line — this job already waited its turn once.</summary>
    private JobTrackerState EnqueueFront(JobId id)
    {
        if (PendingJobs.Contains(id))
            return this;

        return this with { PendingJobs = PendingJobs.Insert(0, id) };
    }

    private JobTrackerState Dequeue(JobId id) =>
        this with { PendingJobs = PendingJobs.Remove(id) };

    private JobTrackerState Reserve(Address address, JobSize amount)
    {
        if (!Nodes.TryGetValue(address, out var node))
            return this;

        return WithNode(node.Reserve(amount));
    }

    private JobTrackerState ReleaseFrom(Address? address, JobSize amount)
    {
        if (address is null)
            return this;

        if (!Nodes.TryGetValue(address, out var node))
            return this;

        return WithNode(node.Release(amount));
    }

    // ------------------------------------------------------------------
    // Projections used to answer queries
    // ------------------------------------------------------------------

    /// <summary>Answers <see cref="JobTrackerQueries.GetJobStatus"/>.</summary>
    public IJobTrackerQueryResponse GetJobStatus(JobId id)
    {
        if (!Jobs.TryGetValue(id, out var job))
            return new JobTrackerQueryResponses.JobNotFound(id);

        return job.ToStatusResult();
    }

    /// <summary>Answers <see cref="JobTrackerQueries.GetQueueStatus"/>.</summary>
    public JobTrackerQueryResponses.QueueStatus GetQueueStatus() => new(
        WaitingCount: PendingJobs.Count,
        RunningCount: Jobs.Values.Count(job => job.Status == JobStatus.Running),
        QueuedWork: Total(WaitingJobs().Select(job => job.Size)),
        TotalCapacity: Total(Nodes.Values.Select(node => node.MaximumCapacity)),
        AvailableCapacity: Total(EligibleNodes().Select(node => node.AvailableCapacity)),
        Nodes: NodesByAvailability());

    private IEnumerable<TrackedJob> WaitingJobs()
    {
        foreach (var id in PendingJobs)
        {
            if (Jobs.TryGetValue(id, out var job))
                yield return job;
        }
    }

    private IEnumerable<TrackedJob> JobsRunningOn(Address address) =>
        Jobs.Values
            .Where(job => job.IsRunningOn(address))
            .OrderBy(job => job.Id.Value, StringComparer.Ordinal);

    private IEnumerable<NodeStatus> EligibleNodes() => Nodes.Values.Where(node => node.IsEligible);

    private ImmutableArray<NodeStatus> NodesByAvailability() =>
        Nodes.Values
            .OrderBy(node => node, MostAvailableCapacityComparer.Instance)
            .ToImmutableArray();

    private static JobSize Total(IEnumerable<JobSize> sizes) =>
        sizes.Aggregate(JobSize.Zero, (running, size) => running + size);
}
