# $HeadURL $
''' EmptyAction

  Action that does nothing.
  
'''

from DIRAC                                                      import gLogger, S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__  = '$Id: $'

class EmptyAction( BaseAction ):
  
  def __init__( self, decissionParams, enforcementResult, singlePolicyResults ):
    
    super( EmptyAction, self ).__init__( decissionParams, enforcementResult, singlePolicyResults )
    
    self.actionName = 'EmptyAction'
  
  def run( self ):
    """Do nothing, but log it :)"""
    
    gLogger.info( '%s: ' % self.actionName )
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
