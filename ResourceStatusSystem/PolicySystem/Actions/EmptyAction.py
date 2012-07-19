# $HeadURL $
''' EmptyAction

  Action that does nothing.
  
'''

from DIRAC                                                      import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__  = '$Id: $'

class EmptyAction( BaseAction ):
  
  def __init__( self, decissionParams, enforcementResult ):
    
    super( EmptyAction, self ).__init__( decissionParams, enforcementResult )
    
    self.actionName = 'EmptyAction'
  
  def run( self ):
    """Do nothing, but log it :)"""
    
    
    gLogger.info( '%s: ' % self.actionName )
    
    #gLogger.info( 'EmptyAction at %s with %s' % (self.name, str(self.pdp_decision)))

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
