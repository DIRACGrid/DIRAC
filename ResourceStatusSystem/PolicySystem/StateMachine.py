# $HeadURL: $
''' StateMachine

  Module that keeps the StateMachine.

'''

from DIRAC import S_OK, S_ERROR

__RCSID__  = '$Id:  $'

class State( object ):
  '''
    State class that represents a single step on a StateMachine, with all the
    possible transitions, the default transition and an ordering level.
  '''
  
  def __init__( self, level, stateMap = list(), defState = None ):
    self.stateMap = stateMap
    self.level    = level
    self.default  = defState

  def transitionRule( self, nextState ):
    '''
      Method that selects next state, knowing the default and the transitions
      map, and the proposed next state.
    '''
    #If next state is on the list of next states, go ahead.
    if nextState in self.stateMap:
      return nextState
    
    #If not, calculate defaultState:
    # if there is a default, that one
    # otherwise is nextState ( states with empty list have no movement restrictions )
    defaultNext = ( 1 and self.default ) or nextState
    return defaultNext
              
class StateMachine( object ):
  '''
    StateMachine class that represents the whole state machine with all transitions.
  '''
  
  def __init__( self, state = None ):
    self.state  = state
    self.states = { 'Nirvana' : State( 100 ) }

  def levelOfState( self, state ):
    '''
      Given a state, it returns its level on the hierarchy
    '''
    
    if not state in self.states:
      return -1
    return self.states[ state ].level

  def setState( self, state ):
    '''
      Makes sure the state is either None or known to the machine
    '''
    
    if state is None:
      self.state = state
    elif state in self.states.keys():
      self.state = state
    else:
      return S_ERROR( '%s is not a valid state' % state )  
    
    return S_OK()

  def getStates( self ):
    '''
      Returns all possible states
    '''
    return self.states.keys()    

  def getNextState( self, candidateState ):
    '''
      - If the candidateState makes no sense, returns error
      - If the state machine has no status, it returns whatever the candidateState is.
      - Otherwise, returns whatever transition makes sense according to the rules      
    '''

    if not candidateState in self.states:
      return S_ERROR( '%s is not a valid state' % candidateState )

    if self.state is None:
      return S_OK( candidateState )
    
    return S_OK( self.states[ self.state ].transitionRule( candidateState ) )

################################################################################
   
class RSSMachine( StateMachine ):
  '''
    RSS implementation of the state machine
  '''
  
  def __init__( self, state ):

    super( RSSMachine, self ).__init__( state )
    
    self.states = { 
                   'Unknown'  : State( 5 ),
                   'Active'   : State( 4 ),
                   'Degraded' : State( 3 ),
                   'Probing'  : State( 2 ),
                   'Banned'   : State( 1, [ 'Banned', 'Probing' ], defState = 'Probing' ),
                   'Error'    : State( 0 )
                  }

  def orderPolicyResults( self, policyResults ):
    '''
    Given a list of policyResults like:
      [ { Status : X, Reason : W ... }, ... ]
    
    Order it by hierarchy  
    '''
    
    policyResults.sort( key = self.levelOfPolicyState )
    #We really do not need to return, as the list is mutable
    #return policyResults
    
  def levelOfPolicyState( self, policyResult ): 
    '''
      Returns the level of the state associated with the policy, -1 if something
      goes wrong. 
    '''
    
    return self.levelOfState( policyResult[ 'Status' ] )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF