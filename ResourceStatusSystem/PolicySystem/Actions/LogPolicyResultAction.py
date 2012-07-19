# $HeadURL:  $
''' LogPolicyResultAction

'''

#from DIRAC                                                      import S_ERROR
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class LogPolicyResultAction( BaseAction ):

  def __init__( self, decissionParams, enforcementResult ):
    
    super( LogPolicyResultAction, self ).__init__( decissionParams, enforcementResult )
    self.actionName = 'LogPolicyResultAction'

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF