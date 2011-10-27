################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC                                           import gLogger  
from DIRAC.Core.DISET.RPCClient                      import RPCClient

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

'''
  Here are all known and relevant APIs for the ResourceStatusSystem Commands
  They can be either clients or RPC servers. 
'''
__APIs__ = {
  'ResourceStatusAPI'     : 'DIRAC.ResourceStatusSystem.API.ResourceStatusAPI',
  'ResourceManagementAPI' : 'DIRAC.ResourceStatusSystem.API.ResourceManagementAPI'          
            }

__Clients__ = {
  'ResourceStatusClient'     : 'DIRAC.ResourceStatusSystem.Client.ResourceStatusClient', 
  'ResourceManagementClient' : 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient',
  'JobsClient'               : 'DIRAC.ResourceStatusSystem.Client.JobsClient',
  'PilotsClient'             : 'DIRAC.ResourceStatusSystem.Client.PilotsClient',
  'ReportsClient'            : 'DIRAC.AccountingSystem.Client.ReportsClient',
  'GOCDBClient'              : 'DIRAC.Core.LCG.GOCDBClient',
  'GGUSTicketsClient'        : 'DIRAC.Core.LCG.GGUSTicketsClient',   
  'SAMResultsClient'         : 'DIRAC.Core.LCG.SAMResultsClient',
  'SLSClient'                : 'DIRAC.Core.LCG.SLSClient' 
             }

__RPCs__ = {
  'WMSAdministrator'         : 'WorkloadManagement/WMSAdministrator',
  'ReportGenerator'          : 'Accounting/ReportGenerator'
             }

################################################################################
################################################################################

def initAPIs( desiredAPIs, knownAPIs ):
  
  APIs = __Clients__.copy()
  APIs.update( __RPCs__ )
  APIs.update( __APIs__ )
  
  if not isinstance( desiredAPIs, list ):
    raise RSSException, 'Got "%s" instead of list while initializing APIs' % desiredAPIs
  
  # Remove duplicated
  desiredAPIs = list(set( desiredAPIs ) )
  
  for dAPI in desiredAPIs:
    
    if knownAPIs.has_key( dAPI ):
      continue
    
    if not dAPI in APIs.keys():
      raise RSSException, '"%s" is not a known client on initAPIs' % dAPI
    
    try:
      
      if not '/' in APIs[ dAPI ]:
        dClientMod = __import__( APIs[ dAPI ], globals(), locals(), ['*'] )
        knownAPIs[ dAPI ] = getattr( dClientMod, dAPI )()
      else:  
        knownAPIs[ dAPI ] = RPCClient( APIs[ dAPI ] )
        
      gLogger.info( 'API %s initialized' % dAPI )
        
    except Exception, x:
      raise RSSException, 'Exception %s while importing "%s"' % ( x, dAPI )  
    
  return knownAPIs

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF