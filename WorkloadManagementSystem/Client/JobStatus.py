"""
Define job statuses with global variables at a single place
"""
__RCSID__ = "$Id$"

SUBMITTING = 'Submitting'
SUBMITTED = 'Submitted'
RECEIVED = 'Received'
CHECKING = 'Checking'
STAGING = 'Staging'
WAITING = 'Waiting'
MATCHED = 'Matched'
RESCHEDULED = 'Rescheduled'
RUNNING = 'Running'
STALLED = 'Stalled'
COMPLETING = 'Completing'
DONE = 'Done'
COMPLETED = 'Completed'
FAILED = 'Failed'
DELETED = 'Deleted'
KILLED = 'Killed'

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

JOB_FINAL_STATES = [DONE,
                    COMPLETED,
                    FAILED]
