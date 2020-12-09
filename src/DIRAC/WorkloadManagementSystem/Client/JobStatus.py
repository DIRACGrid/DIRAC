"""
This module contains constants and lists for the possible job states.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

#:
SUBMITTING = 'Submitting'
#:
SUBMITTED = 'Submitted'
#:
RECEIVED = 'Received'
#:
CHECKING = 'Checking'
#:
STAGING = 'Staging'
#:
WAITING = 'Waiting'
#:
MATCHED = 'Matched'
#:
RESCHEDULED = 'Rescheduled'
#:
RUNNING = 'Running'
#:
STALLED = 'Stalled'
#:
COMPLETING = 'Completing'
#:
DONE = 'Done'
#:
COMPLETED = 'Completed'
#:
FAILED = 'Failed'
#:
DELETED = 'Deleted'
#:
KILLED = 'Killed'

#: Possible job states
JOB_STATES = [SUBMITTING,
              SUBMITTED,
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
              KILLED]

#: Job States when the payload work has finished
JOB_FINAL_STATES = [DONE,
                    COMPLETED,
                    FAILED]
