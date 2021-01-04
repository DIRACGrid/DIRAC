""" StateMachine

  This module contains the basic blocks to build a state machine ( State and
  StateMachine ). And the RSS implementation of it, using its own states map.

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
      >>> s0 = State( 100 )
      >>> s1 = State( 0, [ 'StateName1', 'StateName2' ], defState = 'StateName1' )
      >>> s2 = State( 0, [ 'StateName1', 'StateName2' ] )
      # this example is tricky. The transition rule says that will go to
      # nextState, e.g. 'StateNext'. But, it is not on the stateMap, and there
      # is no default defined, so it will end up going to StateNext anyway. You
      # must be careful while defining states and their stateMaps and defaults.

  :param int level: each state is mapped to an integer, which is used to sort the states according to that integer.
  :param stateMap: it is a list ( of strings ) with the reachable states from this particular
                   status. If not defined, we assume there are no restrictions.
  :type stateMap: python:list
  :param defState: default state used in case the next state it is not stateMap ( not defined
        or simply not there ).
  :type defState: None or str

  """

  def __init__(self, level, stateMap=list(), defState=None):
    """
    Constructor.
    """

    self.level = level
    self.stateMap = stateMap
    self.default = defState

  def transitionRule(self, nextState):
    """
    Method that selects next state, knowing the default and the transitions
    map, and the proposed next state. If <nextState> is in stateMap, goes there.
    If not, then goes to <self.default> if any. Otherwise, goes to <nextState>
    anyway.

    examples:
      >>> s0.transitionRule( 'nextState' )
          'nextState'
      >>> s1.transitionRule( 'StateName2' )
          'StateName2'
      >>> s1.transitionRule( 'StateNameNotInMap' )
          'StateName1'
      >>> s2.transitionRule( 'StateNameNotInMap' )
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
    # otherwise is nextState ( states with empty list have no movement restrictions )
    defaultNext = (1 and self.default) or nextState
    return defaultNext


class StateMachine(object):
  """
    StateMachine class that represents the whole state machine with all transitions.

    examples:
      >>> sm0 = StateMachine()
      >>> sm1 = StateMachine( state = 'Active' )

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

  def levelOfState(self, state):
    """
    Given a state name, it returns its level ( integer ), which defines the hierarchy.

    >>> sm0.levelOfState( 'Nirvana' )
        100
    >>> sm0.levelOfState( 'AnotherState' )
        -1

    :param str state: name of the state, it should be on <self.states> key set
    :return: `int` || -1 ( if not in <self.states> )
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

    :return: list( stateNames )
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
      >>> sm0.getNextState( None )
          S_OK( None )
      >>> sm0.getNextState( 'NextState' )
          S_OK( 'NextState' )

    :param str candidateState: name of the next state
    :return: S_OK( nextState ) || S_ERROR
    """

    if candidateState not in self.states:
      return S_ERROR('%s is not a valid state' % candidateState)

    # FIXME: do we need this anymore ?
    if self.state is None:
      return S_OK(candidateState)

    return S_OK(self.states[self.state].transitionRule(candidateState))

# ...............................................................................


class RSSMachine(StateMachine):
  """
  RSS implementation of the State Machine. It defines six states, which ordered
  by level conform the following list ( higher level first ): Unknown, Active,
  Degraded, Probing, Banned, Error.

  The StateMachine allows any transition except if the current state is Banned,
  which will force any transition to any state different of Error, Banned and
  Probing to Probing.

    examples:
      >>> rsm0 = RSSMachine( None )
      >>> rsm1 = RSSMachine( 'Unknown' )

  :param state: name of the current state of the StateMachine
  :type state: None or str

  """

  def __init__(self, state):
    """
    Constructor.

    """

    super(RSSMachine, self).__init__(state)

    # Defines state map.
    self.states = {
        'Unknown': State(5),
        'Active': State(4),
        'Degraded': State(3),
        'Probing': State(2),
        'Banned': State(1, ['Error', 'Banned', 'Probing'], defState='Probing'),
        'Error': State(0)
    }

  def orderPolicyResults(self, policyResults):
    """
    Method built specifically to interact with the policy results obtained on the
    PDP module. It sorts the input based on the level of their statuses, the lower
    the level state, the leftmost position in the list. Beware, if any of the statuses
    is not know to the StateMachine, it will be ordered first, as its level will be
    -1 !.

    examples:
      >>> rsm0.orderPolicyResults( [ { 'Status' : 'Active', 'A' : 'A' },
                                     { 'Status' : 'Banned', 'B' : 'B' } ] )
          [ { 'Status' : 'Banned', 'B' : 'B' }, { 'Status' : 'Active', 'A' : 'A' } ]
      >>> rsm0.orderPolicyResults( [ { 'Status' : 'Active', 'A' : 'A' },
                                     { 'Status' : 'Rubbish', 'R' : 'R' } ] )
          [ { 'Status' : 'Rubbish', 'R' : 'R' }, { 'Status' : 'Active', 'A' : 'A' } ]


    :param policyResults: list of dictionaries to be ordered. The dictionary can have any key as
        far as the key `Status` is present.
    :type policyResults: python:list

    :result: list( dict ), which is ordered
    """

    # We really do not need to return, as the list is mutable
    policyResults.sort(key=self.levelOfPolicyState)

  def levelOfPolicyState(self, policyResult):
    """
    Returns the level of the state associated with the policy, -1 if something
    goes wrong. It is mostly used while sorting policies with method `orderPolicyResults`.

    examples:
      >>> rsm0.levelOfPolicyState( { 'Status' : 'Active', 'A' : 'A' } )
          5
      >>> rsm0.levelOfPolicyState( { 'Status' : 'Rubbish', 'R' : 'R' } )
          -1

    :param dict policyResult: dictionary that must have the `Status` key.
    :return: int || -1 ( if policyResult[ 'Status' ] is not known by the StateMachine )
    """

    return self.levelOfState(policyResult['Status'])

# ...............................................................................
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
