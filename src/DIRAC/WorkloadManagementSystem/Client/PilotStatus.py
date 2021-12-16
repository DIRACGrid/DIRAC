"""
This module contains constants and lists for the possible pilot states.
"""
#: The pilot has been generated and is transferred to a remote site:
SUBMITTED = "Submitted"
#: The pilot is waiting for a computing resource in a batch queue:
WAITING = "Waiting"
#: The pilot is running a payload on a worker node:
RUNNING = "Running"
#: The pilot finished its execution:
DONE = "Done"
#: The pilot execution failed:
FAILED = "Failed"
#: The pilot was deleted:
DELETED = "Deleted"
#: The pilot execution was aborted:
ABORTED = "Aborted"
#: Cannot get information about the pilot status:
UNKNOWN = "Unknown"

# Note: 'Scheduled' is deprecated and should disappear in future releases
#: Possible pilot states:
PILOT_STATES = [SUBMITTED, WAITING, RUNNING, DONE, FAILED, DELETED, ABORTED, UNKNOWN, "Scheduled"]

#: Waiting states:
PILOT_WAITING_STATES = [SUBMITTED, WAITING, "Scheduled"]

#: Transient states:
PILOT_TRANSIENT_STATES = list(PILOT_WAITING_STATES + [RUNNING, UNKNOWN])


#: Final states:
PILOT_FINAL_STATES = [DONE, ABORTED, DELETED, FAILED]
