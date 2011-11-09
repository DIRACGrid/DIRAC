################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC                                           import gLogger  
#from DIRAC.Core.DISET.RPCClient                      import RPCClient

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

'''
  Here are all known and relevant APIs for the ResourceStatusSystem Commands
  They can be either clients or RPC servers. 
'''
__APIs__ = {         
  'ResourceStatusClient'     : 'DIRAC.ResourceStatusSystem.Client.mock.ResourceStatusClient', 
  'ResourceManagementClient' : 'DIRAC.ResourceStatusSystem.Client.mock.ResourceManagementClient',
  'JobsClient'               : 'DIRAC.ResourceStatusSystem.Client.mock.JobsClient',
  'PilotsClient'             : 'DIRAC.ResourceStatusSystem.Client.mock.PilotsClient',
  'ReportsClient'            : 'DIRAC.AccountingSystem.Client.mock.ReportsClient',
  'GOCDBClient'              : 'DIRAC.Core.LCG.mock.GOCDBClient',
  'GGUSTicketsClient'        : 'DIRAC.Core.LCG.mock.GGUSTicketsClient',   
  'SAMResultsClient'         : 'DIRAC.Core.LCG.mock.SAMResultsClient',
  'SLSClient'                : 'DIRAC.Core.LCG.mock.SLSClient', 
  'WMSAdministrator'         : 'DIRAC.WorkloadManagementSystem.Service.mock.WMSAdministrator',#'WorkloadManagement/WMSAdministrator',
  'ReportGenerator'          : 'DIRAC.AccountingSystem.Service.mock.ReportGenerator'#'Accounting/ReportGenerator'
             }

################################################################################
################################################################################

def initAPIs( desiredAPIs, knownAPIs ):
   
  if not isinstance( desiredAPIs, list ):
    raise RSSException, 'Got "%s" instead of list while initializing APIs' % desiredAPIs
  
  # Remove duplicated
  desiredAPIs = list(set( desiredAPIs ) )
  
  for dAPI in desiredAPIs:
    
    if knownAPIs.has_key( dAPI ):
      continue
    
    if not dAPI in __APIs__.keys():
      raise RSSException, '"%s" is not a known client on initAPIs' % dAPI
    
    try:
      
      if not '/' in __APIs__[ dAPI ]:
        dClientMod = __import__( __APIs__[ dAPI ], globals(), locals(), ['*'] )
        knownAPIs[ dAPI ] = getattr( dClientMod, dAPI )()
        
      gLogger.info( 'API %s initialized' % dAPI )
        
    except Exception:
      knownAPIs[ dAPI ] = None
    
  return knownAPIs

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF