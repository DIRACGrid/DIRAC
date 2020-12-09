########################################################################
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/07/03 10:33:02
########################################################################
"""
:mod: State

.. module: State


:synopsis: state machine

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

state machine
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


class State(object):
  """
  .. class:: State

  single state
  """

  def __str__(self):
    """ str() op """
    return self.__class__.__name__


class StateMachine(object):
  """
  .. class:: StateMachine

  simple state machine

  """

  def __init__(self, state=None, transTable=None):
    """ c'tor

    :param self: self reference
    :param mixed state: initial state
    :param dict transTable: transition table
    """

    if not issubclass(state.__class__, State):
      raise TypeError("state should be inherited from State")
    self.__state = state
    self.transTable = transTable if isinstance(transTable, dict) else {}

  def setState(self, state):
    """ set state """
    assert issubclass(state.__class__, State)
    self.__state = state

  def addTransition(self, fromState, toState, condition):
    """ add transtion rule from :fromState: to :toState: upon condition :condition: """
    if not callable(condition):
      raise TypeError("condition should be callable")
    if not issubclass(fromState.__class__, State):
      raise TypeError("fromState should be inherited from State")
    if not issubclass(toState.__class__, State):
      raise TypeError("toState should be inherited from State")

    if fromState not in self.transTable:
      self.transTable[fromState] = {}
    self.transTable[fromState][toState] = condition

  def next(self, *args, **kwargs):
    """ make transition to the next state

    :param tuple args: args passed to condition
    :param dict kwargs: kwargs passed to condition
    """
    for nextState, condition in self.transTable[self.__state].items():
      if condition(*args, **kwargs):
        self.__state = nextState
        break

  @property
  def state(self):
    """ get current state """
    return self.__state

  @state.setter
  def state(self, state):
    assert issubclass(state.__class__, State)
    self.__state = state


# class Waiting( State ):
#  pass

# class Done( State ):
#  pass

# class Failed( State ):
#  pass

# class Scheduled( State ):
#  pass

# waiting = Waiting()
# done = Done()
# failed = Failed()
# scheduled = Scheduled()

# def toDone( slist ):
#  return list(set(slist)) == [ "Done" ]

# def toFailed( slist ):
#  return "Failed" in slist

# def toWaiting( slist ):
#  for st in slist:
#    if st == "Done":
#      continue
#    if st in ( "Failed", "Scheduled", "Queued" ):
#      return False
#    if st == "Waiting":
#      return True
#  return False

# def toScheduled( slist ):
#  for st in slist:
#    if st == "Done":
#      continue
#    if st in ( "Failed", "Waiting", "Queued" ):
#      return False
#    if st == "Scheduled":
#      return True
#  return False
"""
tr = { waiting: { done: toDone,
                  failed: toFailed,
                  scheduled: toScheduled,
                  waiting: toWaiting },
       scheduled: { failed: toFailed,
                    waiting: toWaiting,
                    done: toDone,
                    scheduled: toScheduled },
       done: { waiting: toWaiting,
               failed: toFailed,
               scheduled: toScheduled,
               done: toDone },
       failed: { waiting: toWaiting,
                 scheduled: toScheduled,
                 done: toDone,
                 failed: toFailed } }

sm = StateMachine( waiting, tr )

sm.setState( scheduled )
print sm.state

sm.next( slist = [ "Waiting", "Queued", "Queued", "Queued" ] )
print sm.state
sm.next( slist = [ "Done", "Queued", "Queued", "Queued" ] )
print sm.state
sm.next( slist = [ "Done", "Done", "Waiting", "Queued" ] )
print sm.state
sm.next( slist = [ "Done", "Done", "Scheduled", "Queued" ] )
print sm.state
sm.next( slist = [ "Done", "Done", "Done", "Waiting" ] )
print sm.state
sm.next( slist = [ "Done", "Done", "Done", "Failed" ] )
print sm.state


"""
