################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC import gLogger

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
  'WMSAdministrator'         : 'DIRAC.WorkloadManagementSystem.Service.mock.WMSAdministrator',#'WorkloadManagement/WMSAdministrator',
  'ReportGenerator'          : 'DIRAC.AccountingSystem.Service.mock.ReportGenerator'#'Accounting/ReportGenerator'
             }

################################################################################
################################################################################

def initAPIs( desiredAPIs, knownAPIs ):

  if not isinstance( desiredAPIs, list ):
    gLogger.error( 'Got "%s" instead of list while initializing APIs' % desiredAPIs )
    return knownAPIs

  # Remove duplicated
  desiredAPIs = list(set( desiredAPIs ) )

  for dAPI in desiredAPIs:

    if knownAPIs.has_key( dAPI ):
      continue

    if not dAPI in __APIs__.keys():
      gLogger.error( '"%s" is not a known client on initAPIs' % dAPI )
      return knownAPIs

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