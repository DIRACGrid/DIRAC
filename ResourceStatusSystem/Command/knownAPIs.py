## $HeadURL $
#''' knownAPIs
#
#  Module used to speed up API instantiation.
#
#'''
#
#from DIRAC                                           import gLogger
#from DIRAC.Core.DISET.RPCClient                      import RPCClient
#
#__RCSID__ = '$Id: $'
#
#'''
#  Here are all known and relevant APIs for the ResourceStatusSystem Commands
#  They can be either clients or RPC servers.
#'''
#__APIs__ = {
#  'ResourceStatusClient'     : 'DIRAC.ResourceStatusSystem.Client.ResourceStatusClient',
#  'ResourceManagementClient' : 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient',
#  'JobsClient'               : 'DIRAC.ResourceStatusSystem.Client.JobsClient',
#  'PilotsClient'             : 'DIRAC.ResourceStatusSystem.Client.PilotsClient',
#  'ReportsClient'            : 'DIRAC.AccountingSystem.Client.ReportsClient',
#  'GOCDBClient'              : 'DIRAC.Core.LCG.GOCDBClient',
#  'GGUSTicketsClient'        : 'DIRAC.Core.LCG.GGUSTicketsClient',
#  'SAMResultsClient'         : 'DIRAC.Core.LCG.SAMResultsClient',
#  'WMSAdministrator'         : 'WorkloadManagement/WMSAdministrator',
#  'ReportGenerator'          : 'Accounting/ReportGenerator'
#             }
#
#################################################################################
#################################################################################
#
#def initAPIs( desiredAPIs, knownAPIs, force = False ):
#
#  if not isinstance( desiredAPIs, list ):
#    gLogger.error( 'Got "%s" instead of list while initializing APIs' % desiredAPIs )
#    return knownAPIs
#
#  # Remove duplicated
#  desiredAPIs = list(set( desiredAPIs ) )
#
#  for dAPI in desiredAPIs:
#
#    if knownAPIs.has_key( dAPI ) and not force == True:
#      continue
#
#    if not dAPI in __APIs__.keys():
#      gLogger.error( '"%s" is not a known client on initAPIs' % dAPI )
#      return knownAPIs
#
#    try:
#
#      if not '/' in __APIs__[ dAPI ]:
#        dClientMod = __import__( __APIs__[ dAPI ], globals(), locals(), ['*'] )
#        knownAPIs[ dAPI ] = getattr( dClientMod, dAPI )()
#      else:
#        knownAPIs[ dAPI ] = RPCClient( __APIs__[ dAPI ] )
#
#      gLogger.info( 'API %s initialized' % dAPI )
#
#    except Exception, x:
#      gLogger.exception( 'Exception %s while importing "%s - %s"' % ( x, dAPI, __APIs__[ dAPI ] ) )
#
#  return knownAPIs
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF