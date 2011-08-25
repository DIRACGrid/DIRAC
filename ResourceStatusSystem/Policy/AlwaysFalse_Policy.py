########################################################################
# $HeadURL:
########################################################################

""" The AlwaysFalse_Policy class is a policy class that... checks nothing!
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class AlwaysFalse_Policy( PolicyBase ):

  def evaluate( self, args, commandIn = None, knownInfo = None ):
    """ 
    Does nothing.
    
    Always returns:
        { 
          'SAT':False,
          'Status':args[2]
          'Reason':None 
        }
    """

    return {'SAT':False, 'Status':args[2], 'Reason':'None'}

  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__
