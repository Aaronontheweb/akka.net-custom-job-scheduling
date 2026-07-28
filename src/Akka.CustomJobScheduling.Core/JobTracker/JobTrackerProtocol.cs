namespace Akka.CustomJobScheduling.Core.JobTracker;

/// <summary>
/// Marker interface for all serializable messages and state in the job tracker domain
/// </summary>
public interface IJobTrackerDomain;

/// <summary>
/// Instructions for the tracker to do something. Validated before they're processed, and rejectable.
/// Commands never change state directly — <see cref="JobTrackerState.Decide"/> turns them into
/// events, and only events change state.
/// </summary>
public interface IJobTrackerCommand : IJobTrackerDomain;

/// <summary>
/// Acknowledgement that a command was accepted or rejected. Goes back to the command's sender.
/// </summary>
public interface IJobTrackerCommandResponse : IJobTrackerDomain;

/// <summary>
/// Something that has already happened, reported to the tracker from outside: a worker's progress, a
/// node joining or leaving, the current membership. A fact is never validated and never rejected —
/// "no" is not a legal answer to a thing that occurred. <see cref="JobTrackerState.Integrate"/> turns
/// a fact into the events it implies, which may be none if it's stale or a duplicate.
/// </summary>
/// <remarks>
/// Distinct from <see cref="IJobTrackerEvent"/> on purpose. A fact is an <i>input</i> the tracker
/// reacts to; an event is what the tracker <i>commits</i> to its journal. One incoming fact can imply
/// several events (a <c>NodeLeft</c> becomes a <c>NodeRemoved</c> plus a requeue per orphaned job).
/// Keeping the types apart is what stops a fact from ever being fed to <see cref="JobTrackerState.Apply"/>,
/// which only folds committed events.
/// </remarks>
public interface IJobTrackerFact : IJobTrackerDomain;

/// <summary>
/// Things that have already happened. Never validated, never rejected — just folded into state by
/// <see cref="JobTrackerState.Apply"/>. These are what gets persisted, so changing one is a schema
/// change.
/// </summary>
public interface IJobTrackerEvent : IJobTrackerDomain;

/// <summary>
/// Read-only instructions to fetch state now, or to be notified about changes when / if / as they
/// happen.
/// </summary>
public interface IJobTrackerQuery : IJobTrackerDomain;

/// <summary>Reply to an <see cref="IJobTrackerQuery"/>.</summary>
public interface IJobTrackerQueryResponse : IJobTrackerDomain;

/// <summary>
/// Unsolicited pushes to subscribers and to the original submitter. Deliberately separate from
/// <see cref="IJobTrackerEvent"/>: events are our persisted schema, notifications are the public
/// contract. Keeping them apart lets us reshape the journal without breaking subscribers.
/// </summary>
public interface IJobTrackerNotification : IJobTrackerDomain;
