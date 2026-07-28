using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using static Akka.CustomJobScheduling.Core.JobTracker.JobTrackerCommands;
using static Akka.CustomJobScheduling.Core.JobTracker.JobTrackerFacts;
using static Akka.CustomJobScheduling.Core.JobTracker.JobTrackerResponses;

namespace Akka.CustomJobScheduling.Core.Tests;

/// <summary>
/// Exercises the full job tracker state machine without an <c>ActorSystem</c>, a <c>TestKit</c>, or
/// a single <c>await</c>. That's the payoff of keeping <see cref="JobTrackerState"/> free of actor
/// concerns: the scheduling rules are just functions, so they're tested like functions.
/// </summary>
public class JobTrackerStateTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly JobSubmitterId Submitter = new("submitter-1");
    private static readonly JobSubmitterId OtherSubmitter = new("submitter-2");

    /// <summary>
    /// Stands in for the tracker actor: keeps the current state and the journal, and does nothing
    /// else. Everything interesting happens inside <see cref="JobTrackerState"/>.
    /// </summary>
    private sealed class Tracker
    {
        public JobTrackerState State { get; private set; } = JobTrackerState.Empty;

        public ImmutableArray<IJobTrackerEvent> Journal { get; private set; } =
            ImmutableArray<IJobTrackerEvent>.Empty;

        public JobTrackerDecision Send(IJobTrackerCommand command, DateTimeOffset? at = null)
        {
            var decision = State.Decide(command, at ?? Now);
            Journal = Journal.AddRange(decision.Events);
            State = State.Fold(decision.Events);
            return decision;
        }

        /// <summary>
        /// Facts take the other door: <see cref="JobTrackerState.Integrate"/>, which returns bare
        /// events and can't reject.
        /// </summary>
        public ImmutableArray<IJobTrackerEvent> Send(IJobTrackerFact fact, DateTimeOffset? at = null)
        {
            var events = State.Integrate(fact, at ?? Now);
            Journal = Journal.AddRange(events);
            State = State.Fold(events);
            return events;
        }

        public TrackedJob Job(string id) => State.Jobs[new JobId(id)];

        public NodeStatus Node(Address address) => State.Nodes[address];
    }

    private static Address Node(string host) => new("akka", "JobSystem", host, 2552);

    private static JobDefinition Job(string id, uint size) =>
        new(new JobId(id), new JobSize(size));

    private static NodeJoined Joining(string host, uint capacity) =>
        new(Node(host), MemberStatus.Up, new JobSize(capacity));

    // ------------------------------------------------------------------
    // Submission
    // ------------------------------------------------------------------

    [Fact]
    public void Submitting_a_job_with_no_nodes_queues_it()
    {
        var tracker = new Tracker();

        var decision = tracker.Send(new SubmitJob(Job("job-1", 10), Submitter));

        Assert.IsType<CommandAccepted>(decision.Response);
        Assert.Equal(JobStatus.Waiting, tracker.Job("job-1").Status);
        Assert.Null(tracker.Job("job-1").AssignedNode);
        Assert.Equal([new JobId("job-1")], tracker.State.PendingJobs);
    }

    [Fact]
    public void Submitting_the_same_job_twice_is_rejected()
    {
        var tracker = new Tracker();
        tracker.Send(new SubmitJob(Job("job-1", 10), Submitter));

        var decision = tracker.Send(new SubmitJob(Job("job-1", 10), Submitter));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.DuplicateJobId, rejected.Reason);
        Assert.Empty(decision.Events);
    }

    [Fact]
    public void An_oversized_job_is_placed_and_run_rather_than_refused()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 50));

        // 100 units on a 50-unit node. It can't fit, but it isn't refused — it's placed on the only
        // node and runs there exclusively. Size is an execution-time problem, not a validity one.
        var decision = tracker.Send(new SubmitJob(Job("job-1", 100), Submitter));

        Assert.IsType<CommandAccepted>(decision.Response);
        var job = tracker.Job("job-1");
        Assert.Equal(JobStatus.Running, job.Status);
        Assert.Equal(Node("node-a"), job.AssignedNode);
    }

    [Fact]
    public void Submitting_a_job_that_fits_schedules_it_immediately()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));

        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);
        Assert.Empty(tracker.State.PendingJobs);
        Assert.Equal(new JobSize(40), tracker.Node(Node("node-a")).CapacityInUse);
    }

    // ------------------------------------------------------------------
    // Placement
    // ------------------------------------------------------------------

    [Fact]
    public void A_node_joining_drains_the_backlog()
    {
        var tracker = new Tracker();
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));
        Assert.Equal(JobStatus.Waiting, tracker.Job("job-1").Status);

        tracker.Send(Joining("node-a", 100));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);
    }

    [Fact]
    public void Placement_prefers_the_node_with_the_most_available_capacity()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));
        tracker.Send(Joining("node-b", 100));

        tracker.Send(new SubmitJob(Job("job-2", 30), Submitter));

        // node-a has 40 free, node-b has 100 free.
        Assert.Equal(Node("node-b"), tracker.Job("job-2").AssignedNode);
    }

    [Fact]
    public void A_large_job_does_not_block_small_jobs_which_run_on_other_nodes()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 100));

        // Load both nodes so a whole-node job can't fit on either right now.
        tracker.Send(new SubmitJob(Job("filler-a", 60), Submitter));  // -> node-a, runs
        tracker.Send(new SubmitJob(Job("filler-b", 60), Submitter));  // -> node-b, runs

        // A job that needs a whole node. It's committed to one node's queue and waits there...
        tracker.Send(new SubmitJob(Job("big", 100), Submitter));

        // ...while a small job submitted right behind it runs immediately on the other node. This is
        // the whole point of dispatch-to-per-node-queues: no global head-of-line block.
        tracker.Send(new SubmitJob(Job("small", 30), Submitter));

        Assert.Equal(JobStatus.Queued, tracker.Job("big").Status);
        Assert.Equal(JobStatus.Running, tracker.Job("small").Status);
        Assert.NotEqual(tracker.Job("big").AssignedNode, tracker.Job("small").AssignedNode);
    }

    [Fact]
    public void Unreachable_nodes_stop_receiving_new_work()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new NodeReachabilityChanged(Node("node-a"), Reachable: false));

        tracker.Send(new SubmitJob(Job("job-1", 10), Submitter));

        Assert.Equal(JobStatus.Waiting, tracker.Job("job-1").Status);
        Assert.False(tracker.Node(Node("node-a")).Reachable);
    }

    // ------------------------------------------------------------------
    // Cancellation
    // ------------------------------------------------------------------

    [Fact]
    public void Only_the_original_submitter_can_cancel_a_job()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        var decision = tracker.Send(new CancelJob(new JobId("job-1"), OtherSubmitter, "nope"));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.NotSubmitter, rejected.Reason);
        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
    }

    [Fact]
    public void Cancelling_a_running_job_releases_its_capacity_to_the_next_job_in_line()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 80), Submitter));
        tracker.Send(new SubmitJob(Job("job-2", 50), Submitter));

        tracker.Send(new CancelJob(new JobId("job-1"), Submitter, "changed my mind"));

        Assert.Equal(JobStatus.Cancelled, tracker.Job("job-1").Status);
        Assert.Equal(JobStatus.Running, tracker.Job("job-2").Status);
        Assert.Equal(new JobSize(50), tracker.Node(Node("node-a")).CapacityInUse);
        Assert.Empty(tracker.State.PendingJobs);
    }

    [Fact]
    public void Cancelling_a_queued_job_removes_it_from_the_queue()
    {
        var tracker = new Tracker();
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        tracker.Send(new CancelJob(new JobId("job-1"), Submitter, "changed my mind"));

        Assert.Equal(JobStatus.Cancelled, tracker.Job("job-1").Status);
        Assert.Empty(tracker.State.PendingJobs);
    }

    [Fact]
    public void A_finished_job_cannot_be_cancelled()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));
        tracker.Send(new ExecutionCompleted(new JobId("job-1"), Node("node-a")));

        var decision = tracker.Send(new CancelJob(new JobId("job-1"), Submitter, "too late"));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.AlreadyTerminal, rejected.Reason);
    }

    [Fact]
    public void Cancelling_an_unknown_job_is_rejected()
    {
        var tracker = new Tracker();

        var decision = tracker.Send(new CancelJob(new JobId("nope"), Submitter, "?"));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.UnknownJob, rejected.Reason);
    }

    // ------------------------------------------------------------------
    // Worker reports
    // ------------------------------------------------------------------

    [Fact]
    public void Facts_produce_events_but_never_a_reply()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        // A fact goes through Integrate, which hands back only events — there is no reply channel at
        // all, so there is nothing to acknowledge and nothing to dead-letter at the executor that
        // stopped the instant it reported. The type says so: this returns events, not a decision.
        var progress = tracker.Send(new ProgressReported(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(10), new JobSize(40))));

        tracker.Send(new ExecutionCompleted(new JobId("job-1"), Node("node-a")));

        // Silent, but not inert — the events still land.
        Assert.NotEmpty(progress);
        Assert.Equal(JobStatus.Completed, tracker.Job("job-1").Status);
    }

    [Fact]
    public void A_report_from_a_node_that_no_longer_owns_the_job_is_ignored()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        // A fact can't be rejected. A completion reported by the wrong node — the job was requeued
        // elsewhere, say — implies no events and changes nothing. No "no" goes back to anyone.
        var events = tracker.Send(new ExecutionCompleted(new JobId("job-1"), Node("node-b")));

        Assert.Empty(events);
        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
    }

    [Fact]
    public void Progress_reported_by_a_node_that_does_not_own_the_job_is_ignored()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        var events = tracker.Send(new ProgressReported(
            new JobId("job-1"),
            Node("node-b"),
            new WorkProgress(new JobSize(20), new JobSize(40))));

        Assert.Empty(events);
        Assert.Equal(JobSize.Zero, tracker.Job("job-1").Progress.Completed);
    }

    [Fact]
    public void Progress_reports_from_the_owning_node_are_recorded()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        tracker.Send(new ProgressReported(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(10), new JobSize(40))));

        Assert.Equal(0.25m, tracker.Job("job-1").Progress.Fraction);
    }

    [Fact]
    public void Completing_a_job_marks_it_fully_done_and_frees_the_node()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        tracker.Send(new ExecutionCompleted(new JobId("job-1"), Node("node-a")));

        var job = tracker.Job("job-1");
        Assert.Equal(JobStatus.Completed, job.Status);
        Assert.Equal(1m, job.Progress.Fraction);
        Assert.Null(job.AssignedNode);
        Assert.Equal(JobSize.Zero, tracker.Node(Node("node-a")).CapacityInUse);
    }

    [Fact]
    public void A_failed_job_frees_the_node_but_keeps_the_progress_it_made()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));
        tracker.Send(new ProgressReported(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(30), new JobSize(40))));

        tracker.Send(new ExecutionFailed(new JobId("job-1"), Node("node-a"), "boom"));

        Assert.Equal(JobStatus.Faulted, tracker.Job("job-1").Status);
        Assert.Equal(new JobSize(30), tracker.Job("job-1").Progress.Completed);
        Assert.Equal(JobSize.Zero, tracker.Node(Node("node-a")).CapacityInUse);
    }

    // ------------------------------------------------------------------
    // Cluster topology
    // ------------------------------------------------------------------

    [Fact]
    public void Losing_a_node_requeues_everything_it_was_running()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));

        tracker.Send(new NodeLeft(Node("node-a")));

        var job = tracker.Job("job-1");
        Assert.Equal(JobStatus.Waiting, job.Status);
        Assert.Null(job.AssignedNode);
        Assert.Equal(JobSize.Zero, job.Progress.Completed);
        Assert.Equal([new JobId("job-1")], tracker.State.PendingJobs);
        Assert.Empty(tracker.State.Nodes);
    }

    [Fact]
    public void A_requeued_job_lands_on_a_surviving_node()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));
        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);

        tracker.Send(new NodeLeft(Node("node-a")));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(Node("node-b"), tracker.Job("job-1").AssignedNode);
        Assert.Equal(new JobSize(60), tracker.Node(Node("node-b")).CapacityInUse);
    }

    [Fact]
    public void A_requeued_job_goes_to_the_front_of_the_queue()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 100), Submitter));
        tracker.Send(new SubmitJob(Job("job-2", 100), Submitter));

        tracker.Send(new NodeLeft(Node("node-a")));

        // job-1 already waited its turn once; it doesn't go to the back of the line.
        Assert.Equal([new JobId("job-1"), new JobId("job-2")], tracker.State.PendingJobs);
    }

    [Fact]
    public void An_unreachable_node_keeps_its_jobs_until_the_cluster_downs_it()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));
        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);

        // The failure detector loses sight of node-a. We can't tell "dead" from "can't see it", and
        // node-a may still be running this job, so it stays put.
        tracker.Send(new NodeReachabilityChanged(Node("node-a"), Reachable: false));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);
        Assert.Equal(new JobSize(60), tracker.Node(Node("node-a")).CapacityInUse);

        // The downing provider gives up and Akka.Cluster removes the member. Now it's safe to move.
        tracker.Send(new NodeLeft(Node("node-a")));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(Node("node-b"), tracker.Job("job-1").AssignedNode);
    }

    [Fact]
    public void A_healed_partition_puts_a_node_back_to_work_with_its_jobs_intact()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));
        tracker.Send(new NodeReachabilityChanged(Node("node-a"), Reachable: false));

        tracker.Send(new NodeReachabilityChanged(Node("node-a"), Reachable: true));

        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);
        Assert.Equal(new JobSize(60), tracker.Node(Node("node-a")).CapacityInUse);

        // ...and it starts taking new work again without needing to rejoin.
        tracker.Send(new SubmitJob(Job("job-2", 30), Submitter));

        Assert.Equal(Node("node-a"), tracker.Job("job-2").AssignedNode);
    }

    [Fact]
    public void Work_is_dispatched_only_to_reachable_nodes()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 100));
        tracker.Send(new NodeReachabilityChanged(Node("node-b"), Reachable: false));

        // node-b is unreachable, so both jobs are dispatched to node-a — the only eligible node.
        tracker.Send(new SubmitJob(Job("job-1", 80), Submitter));
        tracker.Send(new SubmitJob(Job("job-2", 80), Submitter));

        Assert.Equal(Node("node-a"), tracker.Job("job-1").AssignedNode);
        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);

        // job-2 can't run yet (node-a is full) but it's committed to node-a's queue, never to the
        // unreachable node-b.
        Assert.Equal(Node("node-a"), tracker.Job("job-2").AssignedNode);
        Assert.Equal(JobStatus.Queued, tracker.Job("job-2").Status);

        // node-b returns. New work uses it, but job-2 stays where it was committed — assignment is
        // early-binding, jobs don't migrate once queued on a node.
        tracker.Send(new NodeReachabilityChanged(Node("node-b"), Reachable: true));
        tracker.Send(new SubmitJob(Job("job-3", 40), Submitter));

        Assert.Equal(Node("node-b"), tracker.Job("job-3").AssignedNode);
        Assert.Equal(Node("node-a"), tracker.Job("job-2").AssignedNode);
    }

    [Fact]
    public void Re_announcing_a_known_node_does_not_reset_its_committed_capacity()
    {
        var tracker = new Tracker();
        tracker.Send(new NodeJoined(Node("node-a"), MemberStatus.WeaklyUp, new JobSize(100)));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        tracker.Send(new NodeJoined(Node("node-a"), MemberStatus.Up, new JobSize(100)));

        var node = tracker.Node(Node("node-a"));
        Assert.Equal(MemberStatus.Up, node.Status);
        Assert.Equal(new JobSize(40), node.CapacityInUse);
    }

    // ------------------------------------------------------------------
    // Recovery
    // ------------------------------------------------------------------

    [Fact]
    public void Replaying_the_journal_reproduces_the_live_state_exactly()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 60));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));
        tracker.Send(new SubmitJob(Job("job-2", 50), OtherSubmitter));
        tracker.Send(new SubmitJob(Job("job-3", 90), Submitter));
        tracker.Send(new ProgressReported(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(30), new JobSize(60))));
        tracker.Send(new NodeLeft(Node("node-b")));
        tracker.Send(new ExecutionCompleted(new JobId("job-1"), Node("node-a")));

        var recovered = JobTrackerState.Empty.Fold(tracker.Journal);

        Assert.Equal(tracker.State.PendingJobs, recovered.PendingJobs);
        Assert.Equal(tracker.State.Jobs, recovered.Jobs);
        Assert.Equal(tracker.State.Nodes, recovered.Nodes);
    }

    [Fact]
    public void Replay_is_independent_of_when_it_happens()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        // Fold the same journal a year later — nothing in JobTrackerState reads the clock, so the
        // result has to be identical.
        var first = JobTrackerState.Empty.Fold(tracker.Journal);
        var second = JobTrackerState.Empty.Fold(tracker.Journal);

        Assert.Equal(first.Jobs, second.Jobs);
        Assert.Equal(first.Jobs[new JobId("job-1")].LastUpdatedAt, Now);
    }

    // ------------------------------------------------------------------
    // Query projections
    // ------------------------------------------------------------------

    [Fact]
    public void Job_status_query_distinguishes_known_from_unknown_jobs()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        var found = Assert.IsType<JobTrackerQueryResponses.JobStatusResult>(
            tracker.State.GetJobStatus(new JobId("job-1")));
        Assert.Equal(Submitter, found.SubmitterId);
        Assert.Equal(JobStatus.Running, found.Progress.Status);
        Assert.Equal(Node("node-a"), found.AssignedNode);

        Assert.IsType<JobTrackerQueryResponses.JobNotFound>(
            tracker.State.GetJobStatus(new JobId("nope")));
    }

    [Fact]
    public void Queue_status_reports_backlog_and_cluster_capacity()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 60));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));  // -> node-a, runs
        tracker.Send(new SubmitJob(Job("job-2", 50), Submitter));  // -> node-b, runs
        tracker.Send(new SubmitJob(Job("job-3", 90), Submitter));  // only node-a is big enough; queued

        var status = tracker.State.GetQueueStatus();

        // Nothing sits in the global queue — job-3 is committed to node-a's queue, so it's Queued,
        // not Waiting.
        Assert.Equal(0, status.WaitingCount);
        Assert.Equal(1, status.QueuedCount);
        Assert.Equal(2, status.RunningCount);
        Assert.Equal(new JobSize(90), status.QueuedWork);
        Assert.Equal(new JobSize(160), status.TotalCapacity);
        Assert.Equal(new JobSize(50), status.AvailableCapacity);
        Assert.Equal(Node("node-a"), status.Nodes[0].NodeAddress); // most available first
    }

    [Fact]
    public void Job_listing_puts_active_work_first_then_newest_submitted()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));

        // Submitted oldest to newest, one second apart so ordering is unambiguous.
        tracker.Send(new SubmitJob(Job("first", 40), Submitter), Now);
        tracker.Send(new SubmitJob(Job("second", 40), Submitter), Now.AddSeconds(1));
        tracker.Send(new SubmitJob(Job("third", 90), Submitter), Now.AddSeconds(2));

        // Finish the oldest: it should drop below everything still active, not stay at the top.
        tracker.Send(
            new ExecutionCompleted(new JobId("first"), Node("node-a")),
            Now.AddSeconds(3));

        var listed = tracker.State.GetJobs().Jobs.Select(job => job.Id.Value).ToArray();

        Assert.Equal(["third", "second", "first"], listed);
    }

    [Fact]
    public void Progress_does_not_disturb_the_listing_order()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("older", 40), Submitter), Now);
        tracker.Send(new SubmitJob(Job("newer", 40), Submitter), Now.AddSeconds(1));

        var before = tracker.State.GetJobs().Jobs.Select(job => job.Id.Value).ToArray();

        // The whole point of ordering by submission: a progress report moves LastUpdatedAt but
        // must not reshuffle the list, which is what made the dashboard unreadable.
        tracker.Send(
            new ProgressReported(
                new JobId("older"),
                Node("node-a"),
                new WorkProgress(new JobSize(30), new JobSize(40))),
            Now.AddSeconds(9));

        Assert.Equal(before, tracker.State.GetJobs().Jobs.Select(job => job.Id.Value).ToArray());
    }

    [Fact]
    public void Start_time_is_recorded_on_placement_and_cleared_by_a_requeue()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter), Now);

        var running = tracker.Job("job-1");
        Assert.Equal(Now, running.SubmittedAt);
        Assert.Equal(Now, running.StartedAt);

        // A requeue restarts the work elsewhere, so the clock restarts with it — keeping the
        // original start would report a duration spent on a node that no longer exists.
        tracker.Send(new NodeLeft(Node("node-a")), Now.AddSeconds(5));

        var requeued = tracker.Job("job-1");
        Assert.Null(requeued.StartedAt);
        Assert.Equal(Now, requeued.SubmittedAt);
    }

    [Fact]
    public void Node_loss_reschedules_both_running_and_queued_jobs()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));

        // With only node-a in the cluster, R runs and Q queues behind it on node-a.
        tracker.Send(new SubmitJob(Job("R", 60), Submitter));
        tracker.Send(new SubmitJob(Job("Q", 60), Submitter));
        Assert.Equal(JobStatus.Running, tracker.Job("R").Status);
        Assert.Equal(JobStatus.Queued, tracker.Job("Q").Status);
        Assert.Equal(Node("node-a"), tracker.Job("Q").AssignedNode);

        // Survivors join. The queued job doesn't migrate — early binding keeps it on node-a.
        tracker.Send(Joining("node-b", 100));
        tracker.Send(Joining("node-c", 100));
        Assert.Equal(Node("node-a"), tracker.Job("Q").AssignedNode);

        // node-a dies. Both the running job and the merely-queued job must land on survivors —
        // the queued one is exactly the case a running-only requeue would silently orphan.
        tracker.Send(new NodeLeft(Node("node-a")));

        Assert.DoesNotContain(Node("node-a"), tracker.State.Nodes.Keys);

        foreach (var id in new[] { "R", "Q" })
        {
            var job = tracker.Job(id);
            Assert.Contains(job.AssignedNode, new[] { Node("node-b"), Node("node-c") });
            Assert.NotEqual(JobStatus.Waiting, job.Status); // rescheduled, not stranded
        }
    }

    [Fact]
    public void Previously_running_jobs_reschedule_ahead_of_previously_queued_ones()
    {
        var tracker = new Tracker();

        // node-a alone at first, so R (70) runs and Q (40) can't fit the remaining 30 and queues.
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("R", 70), Submitter));
        tracker.Send(new SubmitJob(Job("Q", 40), Submitter));
        Assert.Equal(JobStatus.Running, tracker.Job("R").Status);
        Assert.Equal(JobStatus.Queued, tracker.Job("Q").Status);

        // Two survivors. node-c is small (max 40) with 20 already used, so it can't hold R at all
        // and has no room to run Q. node-b is the only node R fits, and the only one with capacity.
        tracker.Send(Joining("node-c", 40));
        tracker.Send(new SubmitJob(Job("filler", 20), Submitter)); // -> node-c
        Assert.Equal(Node("node-c"), tracker.Job("filler").AssignedNode);
        tracker.Send(Joining("node-b", 100));

        // node-a dies. Both R and Q reschedule, but there's room to *run* only one right now.
        // Because requeue dispatches previously-running jobs first, R claims node-b and runs; Q,
        // dispatched after, lands on node-c and has to wait.
        //
        // The dispatch order is load-bearing here: had Q been dispatched first it would have taken
        // node-b and run, leaving R to wait. Within a single node, submission time still decides —
        // this guarantee is about which survivor each job claims, not intra-node ordering.
        tracker.Send(new NodeLeft(Node("node-a")));

        Assert.Equal(Node("node-b"), tracker.Job("R").AssignedNode);
        Assert.Equal(JobStatus.Running, tracker.Job("R").Status);

        Assert.Equal(Node("node-c"), tracker.Job("Q").AssignedNode);
        Assert.Equal(JobStatus.Queued, tracker.Job("Q").Status);
    }

    [Fact]
    public void Requeuing_a_queued_job_leaves_node_capacity_accounting_intact()
    {
        // A queued job never consumed CapacityInUse. Cancelling or requeuing it must release
        // nothing — releasing would drop the node's InUse below the work actually running on it.
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("R", 70), Submitter));  // runs on node-a
        tracker.Send(new SubmitJob(Job("Q", 50), Submitter));  // 30 free < 50, queues on node-a

        Assert.Equal(JobStatus.Queued, tracker.Job("Q").Status);
        Assert.Equal(new JobSize(70), tracker.Node(Node("node-a")).CapacityInUse);

        tracker.Send(new CancelJob(new JobId("Q"), Submitter, "never mind"));

        Assert.Equal(JobStatus.Cancelled, tracker.Job("Q").Status);
        Assert.Equal(new JobSize(70), tracker.Node(Node("node-a")).CapacityInUse);
    }

    [Fact]
    public void The_notification_pushed_to_subscribers_carries_the_submitter_for_shard_routing()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        var notification = tracker.Job("job-1").ToNotification();

        Assert.Equal(Submitter, notification.SubmitterId);
        Assert.Equal(new JobId("job-1"), notification.Id);
        Assert.Equal(JobStatus.Running, notification.Progress.Status);
    }
}
