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
  
  # Empty pol tyle leads to nothing,
  # so let's do nothing
  
  gLogger.info( 'EmptyAction at %s with %s' % ( name, str( resDecisions ) ) )

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
  