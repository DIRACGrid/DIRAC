# $HeadURL $
''' AlwaysActive_Policy
 
  The AlwaysActive_Policy class is a policy class that... checks nothing!
  
'''

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: $'

class AlwaysActive_Policy( PolicyBase ):

  def evaluate( self ):
    """
    Does nothing.

    Always returns:
        {
          'Status':Active
          'Reason':AlwaysFalse_Policy
        }
    """

    policyResult = { 
                     'Status' : 'Active', 
                     'Reason' : 'This is the AlwasyActive policy' 
                   }
    
    return S_OK( policyResult )
  
################################################################################ 
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF