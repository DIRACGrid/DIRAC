# $HeadURL $
''' AlwaysFalse_Policy
 
  The AlwaysFalse_Policy class is a policy class that... checks nothing!
  
'''

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

__RCSID__ = '$Id: $'

class AlwaysFalse_Policy( PolicyBase ):

  def evaluate( self, commandIn = None, knownInfo = None ):
    """
    Does nothing.

    Always returns:
        {
          'Status':Active
          'Reason':AlwaysFalse_Policy
        }
    """

    return { 'Status' : 'Active', 'Reason' : 'This is the AlwasyFalse policy' }

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
  
################################################################################ 
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF