""" StateMachine

  This module contains the basic blocks to build a state machine (State and StateMachine)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR, gLogger

__RCSID__ = '$Id$'


class State(object):
  """
    State class that represents a single step on a StateMachine, with all the
    possible transitions, the default transition and an ordering level.


    examples:
      >>> s0 = State(100)
      >>> s1 = State(0, ['StateName1', 'StateName2'], defState='StateName1')
      >>> s2 = State(0, ['StateName1', 'StateName2'])
      # this example is tricky. The transition rule says that will go to
      # nextState, e.g. 'StateNext'. But, it is not on the stateMap, and there
      # is no default defined, so it will end up going to StateNext anyway. You
      # must be careful while defining states and their stateMaps and defaults.
  """

  def __init__(self, level, stateMap=None, defState=None):
    """
   :param int level: each state is mapped to an integer, which is used to sort the states according to that integer.
   :param list stateMap: it is a list (of strings) with the reachable states from this particular status.
                         If not defined, we assume there are no restrictions.
   :param str defState: default state used in case the next state is not in stateMap (not defined or simply not there).
    """

    self.level = level
    self.stateMap = stateMap if stateMap else []
    self.default = defState

  def transitionRule(self, nextState):
    """
    Method that selects next state, knowing the default and the transitions
    map, and the proposed next state. If <nextState> is in stateMap, goes there.
    If not, then goes to <self.default> if any. Otherwise, goes to <nextState>
    anyway.

    examples:
      >>> s0.transitionRule('nextState')
          'nextState'
      >>> s1.transitionRule('StateName2')
          'StateName2'
      >>> s1.transitionRule('StateNameNotInMap')
          'StateName1'
      >>> s2.transitionRule('StateNameNotInMap')
          'StateNameNotInMap'

    :param str nextState: name of the state in the stateMap
    :return: state name
    :rtype: str
    """

    # If next state is on the list of next states, go ahead.
    if nextState in self.stateMap:
      return nextState

    # If not, calculate defaultState:
    # if there is a default, that one
    # otherwise is nextState (states with empty list have no movement restrictions)
    defaultNext = self.default if self.default else nextState
    return defaultNext


class StateMachine(object):
  """
    StateMachine class that represents the whole state machine with all transitions.

    examples:
      >>> sm0 = StateMachine()
      >>> sm1 = StateMachine(state = 'Active')

    :param state: current state of the StateMachine, could be None if we do not use the
        StateMachine to calculate transitions. Beware, it is not checked if the
        state is on the states map !
    :type state: None or str

  """

  def __init__(self, state=None):
    """
    Constructor.
    """

    self.state = state
    # To be overwritten by child classes, unless you like Nirvana state that much.
    self.states = {'Nirvana': State(100)}

  def getLevelOfState(self, state):
    """
    Given a state name, it returns its level (integer), which defines the hierarchy.

    >>> sm0.getLevelOfState('Nirvana')
        100
    >>> sm0.getLevelOfState('AnotherState')
        -1

    :param str state: name of the state, it should be on <self.states> key set
    :return: `int` || -1 (if not in <self.states>)
    """

    if state not in self.states:
      return -1
    return self.states[state].level

  def setState(self, candidateState, noWarn=False):
    """ Makes sure the state is either None or known to the machine, and that it is a valid state to move into.
        Final states are also checked.

    examples:
      >>> sm0.setState(None)['OK']
          True
      >>> sm0.setState('Nirvana')['OK']
          True
      >>> sm0.setState('AnotherState')['OK']
          False

    :param state: state which will be set as current state of the StateMachine
    :type state: None or str
    :return: S_OK || S_ERROR
    """

    if candidateState == self.state:
      return S_OK(candidateState)

    if candidateState is None:
      self.state = candidateState
    elif candidateState in self.states:
      if not self.states[self.state].stateMap:
        if not noWarn:
          gLogger.warn("Final state, won't move",
                       "(%s, asked to move to %s)" % (self.state, candidateState))
        return S_OK(self.state)
      if candidateState not in self.states[self.state].stateMap:
        gLogger.warn("Can't move from %s to %s, choosing a good one" % (self.state, candidateState))
      result = self.getNextState(candidateState)
      if not result['OK']:
        return result
      self.state = result['Value']
      # If the StateMachine does not accept the candidate, return error message
    else:
      return S_ERROR("%s is not a valid state" % candidateState)

    return S_OK(self.state)

  def getStates(self):
    """
    Returns all possible states in the state map

    examples:
      >>> sm0.getStates()
          [ 'Nirvana' ]

    :return: list(stateNames)
    """

    return list(self.states)

  def getNextState(self, candidateState):
    """
    Method that gets the next state, given the proposed transition to candidateState.
    If candidateState is not on the state map <self.states>, it is rejected. If it is
    not the case, we have two options: if <self.state> is None, then the next state
    will be <candidateState>. Otherwise, the current state is using its own
    transition rule to decide.

    examples:
      >>> sm0.getNextState(None)
          S_OK(None)
      >>> sm0.getNextState('NextState')
          S_OK('NextState')

    :param str candidateState: name of the next state
    :return: S_OK(nextState) || S_ERROR
    """

    if candidateState not in self.states:
      return S_ERROR('%s is not a valid state' % candidateState)

    # FIXME: do we need this anymore ?
    if self.state is None:
      return S_OK(candidateState)

    return S_OK(self.states[self.state].transitionRule(candidateState))
