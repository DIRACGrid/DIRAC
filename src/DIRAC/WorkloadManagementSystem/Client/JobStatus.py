"""
This module contains constants and lists for the possible job states.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.StateMachine import State, StateMachine


#:
SUBMITTING = "Submitting"
#:
RECEIVED = "Received"
#:
CHECKING = "Checking"
#:
STAGING = "Staging"
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
            DELETED: State(14),  # final state
            KILLED: State(13, [DELETED], defState=KILLED),
            FAILED: State(12, [RESCHEDULED, DELETED], defState=FAILED),
            DONE: State(11, [DELETED], defState=DONE),
            COMPLETED: State(10, [DONE, FAILED], defState=COMPLETED),
            COMPLETING: State(9, [DONE, FAILED, COMPLETED, STALLED, KILLED], defState=COMPLETING),
            STALLED: State(8, [RUNNING, FAILED, KILLED], defState=STALLED),
            RUNNING: State(7, [STALLED, DONE, FAILED, RESCHEDULED, COMPLETING, KILLED, RECEIVED], defState=RUNNING),
            RESCHEDULED: State(6, [WAITING, RECEIVED, DELETED, FAILED], defState=RESCHEDULED),
            MATCHED: State(5, [RUNNING, FAILED, RESCHEDULED, KILLED], defState=MATCHED),
            WAITING: State(4, [MATCHED, RESCHEDULED, DELETED], defState=WAITING),
            STAGING: State(3, [WAITING, FAILED, KILLED], defState=STAGING),
            CHECKING: State(2, [STAGING, WAITING, RESCHEDULED, FAILED, DELETED], defState=CHECKING),
            RECEIVED: State(1, [CHECKING, WAITING, FAILED, DELETED], defState=RECEIVED),
            SUBMITTING: State(0, [RECEIVED, CHECKING, DELETED], defState=SUBMITTING),  # initial state
        }


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
