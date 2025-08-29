from DIRAC.WorkloadManagementSystem.Client import JobStatus


def _filterJobStateTransition(jobStates, candidateState):
    """Given a dictionary of jobs states,
    return a list of jobs that are allowed to transition to the given candidate state.
    """
    allowedJobs = []

    for js in jobStates.items():
        stateRes = JobStatus.JobsStateMachine(js[1]["Status"]).getNextState(candidateState)
        if stateRes["OK"]:
            if stateRes["Value"] == candidateState:
                allowedJobs.append(js[0])
    return allowedJobs
