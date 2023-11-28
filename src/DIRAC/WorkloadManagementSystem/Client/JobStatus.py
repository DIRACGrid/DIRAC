"""
This module contains constants and lists for the possible job states.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.StateMachine import State, StateMachine
from DIRAC.Core.Utilities.Decorators import deprecated


#:
SUBMITTING = "Submitting"
#:
RECEIVED = "Received"
#:
CHECKING = "Checking"
#:
STAGING = "Staging"
#:
SCOUTING = "Scouting"
#:
WAITING = "Waiting"
#:
MATCHED = "Matched"
#: The Rescheduled status is effectively never stored in the DB.
#: It could be considered a "virtual" status, and might even be dropped.
RESCHEDULED = "Rescheduled"
#:
RUNNING = "Running"
#:
STALLED = "Stalled"
#:
COMPLETING = "Completing"
#:
DONE = "Done"
#:
COMPLETED = "Completed"
#:
FAILED = "Failed"
#:
DELETED = "Deleted"
#:
KILLED = "Killed"

#: Possible job states
JOB_STATES = [
    SUBMITTING,
    RECEIVED,
    CHECKING,
    SCOUTING,
    STAGING,
    WAITING,
    MATCHED,
    RESCHEDULED,
    RUNNING,
    STALLED,
    COMPLETING,
    DONE,
    COMPLETED,
    FAILED,
    DELETED,
    KILLED,
]

# Job States when the payload work has finished
JOB_FINAL_STATES = [DONE, COMPLETED, FAILED]

# WMS internal job States indicating the job object won't be updated
JOB_REALLY_FINAL_STATES = [DELETED]


class JobsStateMachine(StateMachine):
    """Jobs state machine"""

    def __init__(self, state):
        """c'tor
        Defines the state machine transactions
        """
        super().__init__(state)

        # States transitions
        self.states = {
            DELETED: State(15),  # final state
            KILLED: State(14, [DELETED], defState=KILLED),
            FAILED: State(13, [RESCHEDULED, DELETED], defState=FAILED),
            DONE: State(12, [DELETED], defState=DONE),
            COMPLETED: State(11, [DONE, FAILED], defState=COMPLETED),
            COMPLETING: State(10, [DONE, FAILED, COMPLETED, STALLED, KILLED], defState=COMPLETING),
            STALLED: State(9, [RUNNING, FAILED, KILLED], defState=STALLED),
            RUNNING: State(8, [STALLED, DONE, FAILED, RESCHEDULED, COMPLETING, KILLED, RECEIVED], defState=RUNNING),
            RESCHEDULED: State(7, [WAITING, RECEIVED, DELETED, FAILED], defState=RESCHEDULED),
            MATCHED: State(6, [RUNNING, FAILED, RESCHEDULED, KILLED], defState=MATCHED),
            WAITING: State(5, [MATCHED, RESCHEDULED, DELETED, KILLED], defState=WAITING),
            STAGING: State(4, [CHECKING, WAITING, FAILED, KILLED], defState=STAGING),
            SCOUTING: State(3, [CHECKING, FAILED, STALLED, KILLED], defState=SCOUTING),
            CHECKING: State(2, [SCOUTING, STAGING, WAITING, RESCHEDULED, FAILED, DELETED], defState=CHECKING),
            RECEIVED: State(1, [SCOUTING, CHECKING, WAITING, FAILED, DELETED], defState=RECEIVED),
            SUBMITTING: State(0, [RECEIVED, CHECKING, DELETED], defState=SUBMITTING),  # initial state
        }


@deprecated("Use filterJobStateTransition instead")
def checkJobStateTransition(jobID, candidateState, currentStatus=None, jobMonitoringClient=None):
    """Utility to check if a job state transition is allowed"""
    if not currentStatus:
        if not jobMonitoringClient:
            from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

            jobMonitoringClient = JobMonitoringClient()

        res = jobMonitoringClient.getJobsStatus(jobID)
        if not res["OK"]:
            return res
        try:
            currentStatus = res["Value"][jobID]["Status"]
        except KeyError:
            return S_ERROR("Job does not exist")

    res = JobsStateMachine(currentStatus).getNextState(candidateState)
    if not res["OK"]:
        return res

    # If the JobsStateMachine does not accept the candidate, return an ERROR
    if candidateState != res["Value"]:
        gLogger.error(
            "Job Status Error",
            f"{jobID} can't move from {currentStatus} to {candidateState}",
        )
        return S_ERROR("Job state transition not allowed")
    return S_OK()


def filterJobStateTransition(jobIDs, candidateState, jobMonitoringClient=None):
    """Given a list of jobIDs, return a list that are allowed to transition
    to the given candidate state.
    """
    allowedJobs = []

    if not isinstance(jobIDs, list):
        jobIDs = [jobIDs]

    if not jobMonitoringClient:
        from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

        jobMonitoringClient = JobMonitoringClient()

    res = jobMonitoringClient.getJobsStatus(jobIDs)
    if not res["OK"]:
        return res

    for jobID in jobIDs:
        if jobID in res["Value"]:
            curState = res["Value"][jobID]["Status"]
            stateRes = JobsStateMachine(curState).getNextState(candidateState)
            if stateRes["OK"]:
                if stateRes["Value"] == candidateState:
                    allowedJobs.append(jobID)
    return S_OK(allowedJobs)
