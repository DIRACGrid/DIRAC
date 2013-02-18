# $HeadURL:  $
''' AlwaysDegradedPolicy module 
'''

from DIRAC                                              import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id:  $'

class AlwaysDegradedPolicy( PolicyBase ):
  '''
    The AlwaysDegradedPolicy is a dummy module that can be used as example, it 
    always returns Degraded status.   
  '''

  @staticmethod
  def _evaluate( commandResult ):
    '''
      It returns Degraded status, evaluates the default command, but its output
      is completely ignored.
    '''

    policyResult = { 
                     'Status' : 'Degraded', 
                     'Reason' : 'This is the AlwaysDegraded policy' 
                   }
    
    return S_OK( policyResult )
  
################################################################################ 
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF