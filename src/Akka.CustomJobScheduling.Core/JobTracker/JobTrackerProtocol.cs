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
