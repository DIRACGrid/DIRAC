# $HeadURL $
''' EmptyAction

  Action that does nothing.
  
'''

from DIRAC                                                      import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.ActionBase import ActionBase

__RCSID__  = '$Id: $'

class EmptyAction( ActionBase ):
  
  def run(self):
    """Do nothing, but log it :)"""
    gLogger.info( 'EmptyAction at %s with %s' % (self.name, str(self.pdp_decision)))

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
