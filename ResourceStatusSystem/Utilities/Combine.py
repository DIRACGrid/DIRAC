"""
DEPRECATED!!! Not used anymore (never been used actually).  Just here
for historical/study purpose.  PDP now use a much versatile and better
system than a hardcoded state machine (see PolicySystem/Status.py).

This module implements the "state machine" and the corresponding
combination function. It is a generic module that has to be
complemented by a list of possible states and transitions (i.e. a
finite automata) that is specific to a VO. If a VO doesn't provide its
own specific automata, then it can use the generic one provided here.

"""
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidStatus

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
  __currentState = None
  __M            = None

  def __init__(self, VOExtension, currentState):

    try:
      module = VOExtension + "DIRAC.ResourceStatusSystem.Policy.Configurations"
      self.__M = __import__(module, locals(), globals(), ['*'])

      # TODO: Develop a real plugin architecture and get rid of that
      # kind of hack. Here: verify that the module has the following
      # values
      getattr(self.__M, "Automata")
      getattr(self.__M, "StateValues")
      getattr(self.__M, "ValidStatus")

    except (ImportError, AttributeError):
      self.__M = __import__("DIRAC.ResourceStatusSystem.PolicySystem.Configurations",
                            locals(), globals(), ['*'])

    if currentState in self.__M.ValidStatus:
      self.__currentState = currentState
    else: raise InvalidStatus()

  def setCurrentState(self, s):
    if s in self.__M.ValidStatus:
      self.__currentState = s
    else: raise InvalidStatus()

  def getCurrentState(self):
    return self.__currentState

  def transitionAllowed(self, s):
    return s in self.__M.Automata[self.__currentState]

  def valueOfStatus(self, s):
    """
    s: a status

    Returns the value of that status
    """
    try: return self.__M.StateValues[s]
    except KeyError: raise InvalidStatus()

  def valueOfPolicy(self, p):
    """
    p: a policy

    Returns the value of the status of that policy
    """
    try: return self.__M.StateValues[p['Status']]
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
