# $HeadURL:  $
''' LogPolicyResultAction

'''

from DIRAC                                                      import S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class LogPolicyResultAction( BaseAction ):

  def __init__( self, decissionParams, enforcementResult, singlePolicyResults ):
    
    super( LogPolicyResultAction, self ).__init__( decissionParams, enforcementResult, singlePolicyResults )
    self.actionName = 'LogPolicyResultAction'

    self.rmClient = ResourceManagementClient()

  def run( self ):
    print 'AA'
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF