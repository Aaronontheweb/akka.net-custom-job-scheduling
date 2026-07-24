using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster;
using Akka.CustomJobScheduling.Core.JobTracker;
using Akka.CustomJobScheduling.Core.Jobs;
using static Akka.CustomJobScheduling.Core.JobTracker.JobTrackerCommands;
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
    public void Submitting_a_job_no_node_could_ever_run_is_rejected()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 50));

        var decision = tracker.Send(new SubmitJob(Job("job-1", 100), Submitter));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.ExceedsClusterCapacity, rejected.Reason);
        Assert.Empty(tracker.State.Jobs);
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
    public void A_job_at_the_front_of_the_queue_blocks_smaller_jobs_behind_it()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 80), Submitter));

        // 20 units free: job-2 doesn't fit, job-3 would — but FIFO means it waits its turn.
        tracker.Send(new SubmitJob(Job("job-2", 50), Submitter));
        tracker.Send(new SubmitJob(Job("job-3", 10), Submitter));

        Assert.Equal(JobStatus.Running, tracker.Job("job-1").Status);
        Assert.Equal(JobStatus.Waiting, tracker.Job("job-2").Status);
        Assert.Equal(JobStatus.Waiting, tracker.Job("job-3").Status);
        Assert.Equal([new JobId("job-2"), new JobId("job-3")], tracker.State.PendingJobs);
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
        tracker.Send(new ReportJobCompleted(new JobId("job-1"), Node("node-a")));

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
    public void Progress_reports_from_a_node_that_does_not_own_the_job_are_rejected()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        var decision = tracker.Send(new ReportProgress(
            new JobId("job-1"),
            Node("node-b"),
            new WorkProgress(new JobSize(20), new JobSize(40))));

        var rejected = Assert.IsType<CommandRejected>(decision.Response);
        Assert.Equal(JobRejectionReason.StaleReport, rejected.Reason);
        Assert.Equal(JobSize.Zero, tracker.Job("job-1").Progress.Completed);
    }

    [Fact]
    public void Progress_reports_from_the_owning_node_are_recorded()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(new SubmitJob(Job("job-1", 40), Submitter));

        tracker.Send(new ReportProgress(
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

        tracker.Send(new ReportJobCompleted(new JobId("job-1"), Node("node-a")));

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
        tracker.Send(new ReportProgress(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(30), new JobSize(40))));

        tracker.Send(new ReportJobFailed(new JobId("job-1"), Node("node-a"), "boom"));

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
    public void Work_queued_while_a_node_was_unreachable_lands_as_soon_as_it_is_removed()
    {
        var tracker = new Tracker();
        tracker.Send(Joining("node-a", 100));
        tracker.Send(Joining("node-b", 100));
        tracker.Send(new NodeReachabilityChanged(Node("node-b"), Reachable: false));
        tracker.Send(new SubmitJob(Job("job-1", 80), Submitter));
        tracker.Send(new SubmitJob(Job("job-2", 80), Submitter));

        // node-b is not eligible, so job-2 waits even though node-b nominally has room.
        Assert.Equal(JobStatus.Waiting, tracker.Job("job-2").Status);

        tracker.Send(new NodeReachabilityChanged(Node("node-b"), Reachable: true));

        Assert.Equal(Node("node-b"), tracker.Job("job-2").AssignedNode);
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
        tracker.Send(new ReportProgress(
            new JobId("job-1"),
            Node("node-a"),
            new WorkProgress(new JobSize(30), new JobSize(60))));
        tracker.Send(new NodeLeft(Node("node-b")));
        tracker.Send(new ReportJobCompleted(new JobId("job-1"), Node("node-a")));

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
        tracker.Send(new SubmitJob(Job("job-1", 60), Submitter));  // -> node-a
        tracker.Send(new SubmitJob(Job("job-2", 50), Submitter));  // -> node-b
        tracker.Send(new SubmitJob(Job("job-3", 90), Submitter));  // queued

        var status = tracker.State.GetQueueStatus();

        Assert.Equal(1, status.WaitingCount);
        Assert.Equal(2, status.RunningCount);
        Assert.Equal(new JobSize(90), status.QueuedWork);
        Assert.Equal(new JobSize(160), status.TotalCapacity);
        Assert.Equal(new JobSize(50), status.AvailableCapacity);
        Assert.Equal(Node("node-a"), status.Nodes[0].NodeAddress); // most available first
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
