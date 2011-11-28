################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  ResourcePolType Actions
"""

from DIRAC import gLogger

################################################################################

def EmptyPolTypeActions( granularity, name, resDecisions, res ):
  
  # Empty pol type leads to nothing,
  # so let's do nothing
  
  gLogger.info( 'EmptyAction at %s with %s' % ( name, str( resDecisions ) ) )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
  