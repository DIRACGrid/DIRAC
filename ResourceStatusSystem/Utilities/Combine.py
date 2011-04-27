"""
This module implements the "state machine" and the corresponding
combination function. It is a generic module that has to be
complemented by a list of possible states and transitions (i.e. a
finite automata) that is specific to a VO. If a VO doesn't provide its
own specific automata, then it can use the generic one provided here.

"""
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidStatus, InvalidStateValueDict

defaultStateValues = {
  'Banned'  : 0,
  'Bad'     : 1,
  'Probing' : 2,
  'Active'  : 3
  }

defaultAutomata = {
  'Active'  : ['Active', 'Bad', 'Banned'],
  'Bad'     : ['Bad', 'Active', 'Banned'],
  'Probing' : ['Probing', 'Active', 'Banned'],
  'Banned'  : ['Banned', 'Probing', 'Active']
  }

class StateMachine(object):
  """
  The class for the state machine. The constructor takes a mandatory
  argument which is the current state which should be in the states
  list. It takes as optional arguments a finite automata defined as a
  dictionnary and a function that defines the values of the states
  defined in the automata. This function has to define a total order
  over the states. The given automata is supposed to be correct, the
  list of the states and the state value function are checked against
  the automata.
  """
  __stateValues  = None
  __automata     = None
  __states       = None
  __currentState = None

  def __init__(self, currentState, stateValues=defaultStateValues, automata=defaultAutomata):
    self.__automata     = automata

    # Check that the defaultStateValues has a value for each state of
    # the automata

    # NOTE: .sort() : Ugly Python 2.x hack because dict.keys() return
    # a list instead of a set. Corrected in Python 3.x
    if(stateValues.keys().sort() == automata.keys().sort()):
      self.__stateValues = stateValues
    else: raise InvalidStateValueDict("%s not equal to %s" % (stateValues.keys(), automata.keys()))

    self.__states       = [st for st in automata]

    if currentState in self.__states:
      self.__currentState = currentState
    else: raise InvalidStatus()

  def setCurrentState(self, s):
    if s in self.__states:
      self.__currentState = s
    else: raise InvalidStatus()

  def getCurrentState(self):
    return self.__currentState

  def transitionAllowed(self, s):
    return s in self.__automata[self.__currentState]

  def valueOfStatus(self, s):
    """
    s: a status

    Returns the value of that status
    """
    try: return self.__stateValues[s]
    except KeyError: raise InvalidStatus()

  def valueOfPolicy(self, p):
    """
    p: a policy

    Returns the value of the status of that policy
    """
    try: return self.__stateValues[p['Status']]
    except KeyError: raise InvalidStatus()

  def combine(self, ps):
    """
    ps: a list of policies

    Sets the current state of the automata at the lowest possible
    state, and returns that state. If no transition is possible,
    return the current state.
    """
    ps.sort(key=self.valueOfPolicy)
    for pol in ps:
      if self.transitionAllowed(pol['Status']):
        self.__currentState = pol['Status']
        return self.__currentState

    # If no transition is possible...
    return self.__currentState
