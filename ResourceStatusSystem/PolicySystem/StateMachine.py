# $HeadURL: $
''' StateMachine

  Module that keeps the StateMachine.

'''

class State( object ):
  
  def __init__( self, level, mapState = dict(), defState = None ):
    self.map     = mapState
    self.level   = level
    self.default = defState
      
  def transitionRule( self, nextState ):
    defaultNext = ( 1 and self.default ) or nextState
    return self.map.get( nextState, defaultNext )

class StateMachine( object ):
  
  def __init__( self ):
    self.states = { 'Nirvana' : State( 100 ) }

  def levelOfState( self, state ):
    
    if not state in self.states:
      return -1
    return self.states[ state ].level

################################################################################
   
class RSSMachine( StateMachine ):
  
  def __init__( self ):
    
    self.states = { 
                   'Unknown'  : State( 5 ),
                   'Active'   : State( 4 ),
                   'Degraded' : State( 3 ),
                   'Probing'  : State( 2 ),
                   'Banned'   : State( 1, { 'Banned' : 'Banned' }, defState = 'Probing' ),
                  }
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF