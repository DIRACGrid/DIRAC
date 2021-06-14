"""
This module contains constants and lists for the possible job states.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.StateMachine import State, StateMachine


#:
SUBMITTING = 'Submitting'
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

# Job States when the payload work has finished
JOB_FINAL_STATES = [DONE,
                    COMPLETED,
                    FAILED]

# WMS internal job States indicating the job object won't be updated
JOB_REALLY_FINAL_STATES = [DELETED]


class JobsStateMachine(StateMachine):
  """ Jobs state machine
  """

  def __init__(self, state):
    """ c'tor
        Defines the state machine transactions
    """
    super(JobsStateMachine, self).__init__(state)

    # States transitions
    self.states = {DELETED: State(14),  # final state
                   KILLED: State(13,
                                 [DELETED],
                                 defState=KILLED),
                   FAILED: State(12,
                                 [RESCHEDULED, DELETED],
                                 defState=FAILED),
                   DONE: State(11,
                               [DELETED],
                               defState=DONE),
                   COMPLETED: State(10,
                                    [DONE, FAILED],
                                    defState=COMPLETED),
                   COMPLETING: State(9,
                                     [DONE, FAILED, COMPLETED],
                                     defState=COMPLETING),
                   STALLED: State(8,
                                  [RUNNING, FAILED, KILLED],
                                  defState=STALLED),
                   RUNNING: State(7,
                                  [STALLED, DONE, FAILED, COMPLETING, KILLED],
                                  defState=RUNNING),
                   RESCHEDULED: State(6,
                                      [WAITING, RECEIVED, DELETED],
                                      defState=RESCHEDULED),
                   MATCHED: State(5,
                                  [RUNNING, KILLED],
                                  defState=MATCHED),
                   WAITING: State(4,
                                  [MATCHED, RESCHEDULED, DELETED],
                                  defState=WAITING),
                   STAGING: State(3,
                                  [WAITING, FAILED, KILLED],
                                  defState=STAGING),
                   CHECKING: State(2,
                                   [STAGING, WAITING, RESCHEDULED, FAILED, DELETED],
                                   defState=CHECKING),
                   RECEIVED: State(1,
                                   [CHECKING, WAITING, FAILED, DELETED],
                                   defState=RECEIVED),
                   SUBMITTING: State(0,  # initial state
                                     [RECEIVED, CHECKING, DELETED],
                                     defState=SUBMITTING)}
