using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.Jobs;

namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// The outcome of handing an <see cref="IJobTrackerCommand"/> to <see cref="JobTrackerState.Decide"/>:
/// the facts to persist, and what to tell the sender.
/// </summary>
/// <remarks>
/// Only commands produce one of these, because only commands can be rejected. A fact goes through
/// <see cref="JobTrackerState.Integrate"/>, which returns bare events and has no room for a rejection.
/// </remarks>
/// <param name="Events">Events to persist and then fold back into the state, in order.</param>
/// <param name="Response">The acknowledgement to send back, or <c>null</c> for a no-op command.</param>
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
/// <c>now</c> parameter and is then carried by the events themselves, which is what makes recovery
/// deterministic: replaying a journal always produces the same state, no matter when you replay it.
/// </para>
/// <para>Three operations, and the type of the input picks which one runs:</para>
/// <list type="bullet">
/// <item><description>
/// <see cref="Decide"/> — takes an <see cref="IJobTrackerCommand"/> (a request), validates it, and
/// returns the events it implies or a rejection. The only place "no" is a legal answer.
/// </description></item>
/// <item><description>
/// <see cref="Integrate"/> — takes an <see cref="IJobTrackerFact"/> (something that already
/// happened), and returns the events it implies. Never rejects; a stale fact returns no events.
/// </description></item>
/// <item><description>
/// <see cref="Apply"/> — folds one <see cref="IJobTrackerEvent"/> into a new state. Pure, per-type,
/// and the only thing that runs during recovery. Never produces events, never rejects.
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
    // Decide: command -> events (or a rejection)
    // ------------------------------------------------------------------

    /// <summary>
    /// Validates <paramref name="command"/> against the current state and returns the events it
    /// implies, or a rejection. Does not modify this instance.
    /// </summary>
    /// <param name="now">
    /// The timestamp to stamp onto resulting events. Supplied by the caller so transitions stay
    /// deterministic and testable.
    /// </param>
    /// <remarks>
    /// Two commands, and that's the whole set — everything else the tracker hears is a fact. A
    /// command that succeeds also drains the queue, so placement lives in one place.
    /// </remarks>
    public JobTrackerDecision Decide(IJobTrackerCommand command, DateTimeOffset now)
    {
        var decision = command switch
        {
            JobTrackerCommands.SubmitJob c => OnSubmitJob(c, now),
            JobTrackerCommands.CancelJob c => OnCancelJob(c, now),
            _ => JobTrackerDecision.None
        };

        if (decision.WasRejected)
            return decision;

        var placements = Fold(decision.Events).PlacePendingJobs(now);
        return decision.Then(placements);
    }

    private JobTrackerDecision OnSubmitJob(JobTrackerCommands.SubmitJob command, DateTimeOffset now)
    {
        // A duplicate id is the only reason to refuse a submission: it's a conflict between two
        // requests. Size is never a reason — an oversized job is accepted and placed like any other,
        // it just runs on a whole node to itself. See SelectDispatchNode.
        if (Jobs.ContainsKey(command.Id))
            return Rejected(command.Id, JobRejectionReason.DuplicateJobId, "already submitted");

        return JobTrackerDecision.Accept(
            command.Id,
            new JobTrackerEvents.JobAccepted(command.Job, command.SubmitterId, now));
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

    private static JobTrackerDecision UnknownJob(JobId id) =>
        Rejected(id, JobRejectionReason.UnknownJob, "not known to this tracker");

    private static JobTrackerDecision Rejected(JobId id, JobRejectionReason reason, string why) =>
        JobTrackerDecision.Reject(id, reason, $"Job '{id.Value}' {why}.");

    // ------------------------------------------------------------------
    // Integrate: fact -> events (never a rejection)
    // ------------------------------------------------------------------

    /// <summary>
    /// Folds an <see cref="IJobTrackerFact"/> into the events it implies, then drains the queue.
    /// Never rejects: a fact already happened, so the worst it can do is imply nothing.
    /// </summary>
    /// <remarks>
    /// The return type is the enforcement. There is no slot for a rejection, so a stale progress
    /// report or a duplicate node-join is simply an empty result — a silent no-op, not a "no".
    /// </remarks>
    public ImmutableArray<IJobTrackerEvent> Integrate(IJobTrackerFact fact, DateTimeOffset now)
    {
        var events = fact switch
        {
            JobTrackerFacts.ProgressReported f => OnProgressReported(f, now),
            JobTrackerFacts.ExecutionCompleted f => OnExecutionCompleted(f, now),
            JobTrackerFacts.ExecutionFailed f => OnExecutionFailed(f, now),
            JobTrackerFacts.NodeJoined f => OnNodeJoined(f, now),
            JobTrackerFacts.NodeLeft f => OnNodeLeft(f, now),
            JobTrackerFacts.NodeReachabilityChanged f => OnNodeReachabilityChanged(f, now),
            JobTrackerFacts.NodesSynced f => OnNodesSynced(f, now),

            // DrainQueue (and anything unrecognised) implies no events on its own. The placement pass
            // below is the entire reason the tick exists.
            _ => ImmutableArray<IJobTrackerEvent>.Empty
        };

        var placements = Fold(events).PlacePendingJobs(now);
        return events.AddRange(placements);
    }

    private ImmutableArray<IJobTrackerEvent> OnProgressReported(
        JobTrackerFacts.ProgressReported fact,
        DateTimeOffset now)
    {
        if (!IsCurrentReport(fact.Id, fact.NodeAddress))
            return [];

        return [new JobTrackerEvents.JobProgressed(fact.Id, fact.Progress, now)];
    }

    private ImmutableArray<IJobTrackerEvent> OnExecutionCompleted(
        JobTrackerFacts.ExecutionCompleted fact,
        DateTimeOffset now)
    {
        if (!IsCurrentReport(fact.Id, fact.NodeAddress))
            return [];

        return [new JobTrackerEvents.JobCompleted(fact.Id, now)];
    }

    private ImmutableArray<IJobTrackerEvent> OnExecutionFailed(
        JobTrackerFacts.ExecutionFailed fact,
        DateTimeOffset now)
    {
        if (!IsCurrentReport(fact.Id, fact.NodeAddress))
            return [];

        return [new JobTrackerEvents.JobFailed(fact.Id, fact.Reason, now)];
    }

    /// <summary>
    /// Is this report about a job actually running on the node it came from?
    /// </summary>
    /// <remarks>
    /// A report for an unknown job, or from a node that no longer owns the job, is stale — the job
    /// was requeued elsewhere, or already finished. There's nobody to tell "no": the report just
    /// implies no events. Progress is absolute, so a lost report self-corrects on the next tick;
    /// the only thing this guards against is an old node's late report resurrecting stale state.
    /// </remarks>
    private bool IsCurrentReport(JobId id, Address from) =>
        Jobs.TryGetValue(id, out var job) && job.IsRunningOn(from);

    private ImmutableArray<IJobTrackerEvent> OnNodeJoined(
        JobTrackerFacts.NodeJoined fact,
        DateTimeOffset now)
    {
        if (!Nodes.TryGetValue(fact.NodeAddress, out var existing))
            return [new JobTrackerEvents.NodeAdded(fact.NodeAddress, fact.Status, fact.MaxCapacity, now)];

        if (existing.Status == fact.Status && existing.Reachable)
            return [];

        // Already known — this is a membership transition (e.g. WeaklyUp -> Up), not a new node.
        // Fold it as a status change so we don't discard the capacity it's already committed to.
        return [new JobTrackerEvents.NodeStatusChanged(fact.NodeAddress, fact.Status, Reachable: true, now)];
    }

    private ImmutableArray<IJobTrackerEvent> OnNodeLeft(
        JobTrackerFacts.NodeLeft fact,
        DateTimeOffset now)
    {
        if (!Nodes.ContainsKey(fact.NodeAddress))
            return [];

        // Requeue everything the node was carrying — running and queued alike — before forgetting
        // the node, so capacity accounting unwinds in the right order. JobsCommittedTo orders
        // previously-running jobs ahead of previously-queued ones, so they reschedule first.
        var events = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        foreach (var job in JobsCommittedTo(fact.NodeAddress))
        {
            events.Add(new JobTrackerEvents.JobRequeued(
                job.Id,
                fact.NodeAddress,
                "Node left the cluster.",
                now));
        }

        events.Add(new JobTrackerEvents.NodeRemoved(fact.NodeAddress, now));

        return events.ToImmutable();
    }

    private ImmutableArray<IJobTrackerEvent> OnNodeReachabilityChanged(
        JobTrackerFacts.NodeReachabilityChanged fact,
        DateTimeOffset now)
    {
        if (!Nodes.TryGetValue(fact.NodeAddress, out var existing))
            return [];

        if (existing.Reachable == fact.Reachable)
            return [];

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
        return [new JobTrackerEvents.NodeStatusChanged(fact.NodeAddress, existing.Status, fact.Reachable, now)];
    }

    /// <summary>
    /// Reconciles the whole node map against the authoritative membership in one step.
    /// </summary>
    /// <remarks>
    /// Needed because the tracker is persistent. Replaying the journal faithfully restores nodes
    /// that were present when the events were written; if one of them left while the tracker was
    /// down, no <see cref="JobTrackerFacts.NodeLeft"/> is ever coming for it, and its jobs would
    /// stay <see cref="JobStatus.Running"/> on a node that isn't there. Removals are emitted before
    /// additions so freed capacity is available to the placement pass that follows.
    /// </remarks>
    private ImmutableArray<IJobTrackerEvent> OnNodesSynced(
        JobTrackerFacts.NodesSynced fact,
        DateTimeOffset now)
    {
        var events = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        var departed = Nodes.Keys
            .Where(address => !fact.Members.ContainsKey(address))
            .OrderBy(address => address.ToString(), StringComparer.Ordinal);

        foreach (var address in departed)
        {
            foreach (var job in JobsCommittedTo(address))
            {
                events.Add(new JobTrackerEvents.JobRequeued(
                    job.Id,
                    address,
                    "Node is no longer a cluster member.",
                    now));
            }

            events.Add(new JobTrackerEvents.NodeRemoved(address, now));
        }

        var arrived = fact.Members
            .Where(member => !Nodes.ContainsKey(member.Key))
            .OrderBy(member => member.Key.ToString(), StringComparer.Ordinal);

        foreach (var (address, capacity) in arrived)
        {
            events.Add(new JobTrackerEvents.NodeAdded(address, MemberStatus.Up, capacity, now));
        }

        return events.ToImmutable();
    }

    // ------------------------------------------------------------------
    // Placement
    // ------------------------------------------------------------------

    /// <summary>
    /// Places whatever it can from the pending queue, in two phases: dispatch every waiting job onto
    /// a node's queue, then start the head of each node's queue that has room to run.
    /// </summary>
    /// <remarks>
    /// The two phases are what avoid head-of-line blocking. A job that needs a whole node doesn't
    /// stall the ones behind it: it's dispatched into one node's queue while smaller jobs are steered
    /// to other nodes and start right away.
    /// </remarks>
    public ImmutableArray<IJobTrackerEvent> PlacePendingJobs(DateTimeOffset now)
    {
        if (Nodes.IsEmpty)
            return ImmutableArray<IJobTrackerEvent>.Empty;

        // Build the placement working set once and mutate it as we go, instead of re-deriving each
        // node's queue and committed load from a full scan of every job on every candidate lookup -
        // which made a placement pass quadratic in the backlog. `inUse` is capacity consumed per node
        // (it only grows in phase 2, as jobs actually start); `queues` holds each node's committed-but-
        // not-running jobs in FIFO order; `queuedSize` is the running total of each queue.
        var inUse = Nodes.ToDictionary(entry => entry.Key, entry => entry.Value.CapacityInUse);
        var queues = Nodes.Keys.ToDictionary(address => address, _ => new List<TrackedJob>());
        var queuedSize = Nodes.Keys.ToDictionary(address => address, _ => JobSize.Zero);

        foreach (var job in Jobs.Values)
        {
            if (job.Status == JobStatus.Queued
                && job.AssignedNode is { } committedTo
                && queues.TryGetValue(committedTo, out var committed))
            {
                committed.Add(job);
                queuedSize[committedTo] += job.Size;
            }
        }

        foreach (var queue in queues.Values)
            queue.Sort(ByQueueOrder);

        var events = ImmutableArray.CreateBuilder<IJobTrackerEvent>();

        // Phase 1 - dispatch every waiting job onto a node's queue. A large job lands in one node's
        // queue and waits there while smaller jobs are steered to other nodes, so nothing head-of-line
        // blocks the global queue.
        foreach (var id in PendingJobs)
        {
            if (!Jobs.TryGetValue(id, out var job) || job.Status != JobStatus.Waiting)
                continue;

            var target = SelectDispatchNode(job.Definition, inUse, queuedSize);
            if (target is null)
                continue; // no eligible node at all right now - leave it waiting

            events.Add(new JobTrackerEvents.JobQueued(id, target, now));
            InsertByQueueOrder(queues[target], job);
            queuedSize[target] += job.Size;
        }

        // Phase 2 - each node runs the head of its own queue while it can. Strict per-node FIFO: if the
        // head doesn't fit yet the node waits and drains toward it rather than running a later job past
        // it. An oversized head runs alone once the node is idle.
        foreach (var address in Nodes.Keys)
        {
            var queue = queues[address];
            var node = Nodes[address];

            while (queue.Count > 0 && node.CanRun(inUse[address], queue[0].Definition))
            {
                var head = queue[0];
                events.Add(new JobTrackerEvents.JobScheduled(head.Id, address, now));
                queue.RemoveAt(0);
                inUse[address] += head.Size;
            }
        }

        return events.ToImmutable();
    }

    /// <summary>
    /// Which node a waiting job is dispatched to, given the load already committed this pass.
    /// </summary>
    /// <remarks>
    /// First choice is the least-loaded eligible node big enough to hold the job, ties broken by
    /// address; "loaded" is running plus already-queued work, so a burst spreads across the cluster.
    /// If nothing is big enough, the job is oversized - it still has to run, so it goes to the roomiest
    /// eligible node and will occupy it exclusively. Null only when there is no eligible node at all,
    /// in which case the job stays waiting.
    /// </remarks>
    private Address? SelectDispatchNode(
        JobDefinition job,
        IReadOnlyDictionary<Address, JobSize> inUse,
        IReadOnlyDictionary<Address, JobSize> queuedSize)
    {
        var eligible = Nodes.Values.Where(node => node.IsEligible).ToList();
        if (eligible.Count == 0)
            return null;

        JobSize Load(NodeStatus node) => inUse[node.NodeAddress] + queuedSize[node.NodeAddress];

        var fitting = eligible
            .Where(node => node.MaximumCapacity >= job.Size)
            .OrderBy(Load)
            .ThenBy(node => node.NodeAddress.ToString(), StringComparer.Ordinal)
            .ToList();

        if (fitting.Count > 0)
            return fitting[0].NodeAddress;

        return eligible
            .OrderByDescending(node => node.MaximumCapacity)
            .ThenBy(Load)
            .ThenBy(node => node.NodeAddress.ToString(), StringComparer.Ordinal)
            .First()
            .NodeAddress;
    }

    /// <summary>FIFO order within a node's queue: submission time, then id.</summary>
    private static int ByQueueOrder(TrackedJob left, TrackedJob right)
    {
        var bySubmission = left.SubmittedAt.CompareTo(right.SubmittedAt);

        return bySubmission != 0
            ? bySubmission
            : string.CompareOrdinal(left.Id.Value, right.Id.Value);
    }

    private static void InsertByQueueOrder(List<TrackedJob> queue, TrackedJob job)
    {
        var index = queue.Count;
        while (index > 0 && ByQueueOrder(queue[index - 1], job) > 0)
            index--;

        queue.Insert(index, job);
    }

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
        JobTrackerEvents.JobQueued e => ApplyJobQueued(e),
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

    private JobTrackerState ApplyJobQueued(JobTrackerEvents.JobQueued e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        // Out of the global queue, into the node's queue. No capacity moves — a queued job isn't
        // running yet, so it doesn't touch CapacityInUse.
        return WithJob(job.QueueOn(e.NodeAddress, e.OccurredAt))
            .Dequeue(e.Id);
    }

    private JobTrackerState ApplyJobScheduled(JobTrackerEvents.JobScheduled e)
    {
        if (!Jobs.TryGetValue(e.Id, out var job))
            return this;

        // The job leaves its node queue and starts running, now consuming capacity. Dequeue is a
        // no-op for the normal Queued -> Running path (queued jobs aren't in the global queue), but
        // stays as a guard.
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

        // Only a job that was actually Running held CapacityInUse on the node. A Queued job was
        // merely committed to the node's queue and never consumed capacity, so releasing here would
        // drop InUse below the real running work and leave the node looking emptier than it is.
        var released = job.Status == JobStatus.Running
            ? ReleaseFrom(e.PreviousNode, job.Size)
            : this;

        // Appended at the back of the global queue. Node-loss emits requeues running-first, and the
        // same pass re-dispatches the whole batch, so append order is the reschedule order — no
        // front-insertion reversal to reason about. A job that can't currently be placed is simply
        // skipped by Dispatch, so it never blocks the ones behind it.
        return released
            .WithJob(job.Requeue(e.OccurredAt))
            .Enqueue(e.Id);
    }

    /// <summary>
    /// Common tail for every terminal transition: record it, drop it from the queue (in case it was
    /// never placed), and hand back any capacity it was actually using.
    /// </summary>
    /// <remarks>
    /// A Running job holds real capacity; a Queued or Waiting job doesn't. Releasing capacity for a
    /// job that never ran would corrupt the node's accounting, so the release is gated on the
    /// pre-transition status.
    /// </remarks>
    private JobTrackerState Finish(TrackedJob before, TrackedJob after)
    {
        var released = before.Status == JobStatus.Running
            ? ReleaseFrom(before.AssignedNode, before.Size)
            : this;

        return released
            .WithJob(after)
            .Dequeue(before.Id);
    }

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

    /// <summary>Answers <see cref="JobTrackerQueries.GetJobs"/>.</summary>
    /// <remarks>
    /// Active work first, then finished, each newest-submitted first. Capped so a long-running
    /// tracker with a large history can't return an unbounded page.
    ///
    /// Deliberately not ordered by last-updated: that moves on every progress tick, so a list
    /// sorted by it reshuffles constantly and is impossible to follow. Submission time never moves,
    /// so a row only changes position when a new job arrives or when it finishes and drops into the
    /// lower group.
    /// </remarks>
    public JobTrackerQueryResponses.JobList GetJobs(bool includeFinished = true, int limit = 200)
    {
        var matching = Jobs.Values.Where(job => includeFinished || !job.IsTerminal).ToList();

        var page = matching
            .OrderBy(job => job.IsTerminal)
            .ThenByDescending(job => job.SubmittedAt)
            .ThenBy(job => job.Id.Value, StringComparer.Ordinal)
            .Take(Math.Max(0, limit))
            .Select(job => job.ToStatusResult())
            .ToImmutableArray();

        return new JobTrackerQueryResponses.JobList(page, matching.Count);
    }

    /// <summary>Answers <see cref="JobTrackerQueries.GetQueueStatus"/>.</summary>
    public JobTrackerQueryResponses.QueueStatus GetQueueStatus()
    {
        var waiting = 0;
        var queued = 0;
        var running = 0;
        var queuedWork = JobSize.Zero;

        // One pass over the jobs for all four aggregates rather than four separate scans.
        foreach (var job in Jobs.Values)
        {
            switch (job.Status)
            {
                case JobStatus.Waiting:
                    waiting++;
                    queuedWork += job.Size;
                    break;
                case JobStatus.Queued:
                    queued++;
                    queuedWork += job.Size;
                    break;
                case JobStatus.Running:
                    running++;
                    break;
            }
        }

        return new JobTrackerQueryResponses.QueueStatus(
            WaitingCount: waiting,
            RunningCount: running,
            QueuedWork: queuedWork,
            TotalCapacity: Total(Nodes.Values.Select(node => node.MaximumCapacity)),
            AvailableCapacity: Total(EligibleNodes().Select(node => node.AvailableCapacity)),
            Nodes: NodesByAvailability(),
            QueuedCount: queued);
    }

    /// <summary>
    /// Every job a node is carrying — running or merely queued — ordered so previously-running jobs
    /// come first. That ordering is what makes a node death reschedule in-flight work ahead of work
    /// that hadn't started, per the requeue path.
    /// </summary>
    private IEnumerable<TrackedJob> JobsCommittedTo(Address address) =>
        Jobs.Values
            .Where(job => job.Status is JobStatus.Running or JobStatus.Queued
                          && Equals(job.AssignedNode, address))
            .OrderBy(job => job.Status == JobStatus.Running ? 0 : 1)
            .ThenBy(job => job.SubmittedAt)
            .ThenBy(job => job.Id.Value, StringComparer.Ordinal);

    private IEnumerable<NodeStatus> EligibleNodes() => Nodes.Values.Where(node => node.IsEligible);

    private ImmutableArray<NodeStatus> NodesByAvailability() =>
        Nodes.Values
            .OrderBy(node => node, MostAvailableCapacityComparer.Instance)
            .ToImmutableArray();

    private static JobSize Total(IEnumerable<JobSize> sizes) =>
        sizes.Aggregate(JobSize.Zero, (running, size) => running + size);
}
