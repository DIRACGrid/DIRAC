""" ResourcePolType Actions
"""

from DIRAC import gLogger

def EmptyPolTypeActions( granularity, name, resDecisions, res ):
  
  # Empty pol tyle leads to nothing,
  # so let's do nothing
  
  gLogger.info( 'EmptyAction at %s with %s \n' % ( name, str( resDecisions ) ) )

  