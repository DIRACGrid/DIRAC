#!/usr/bin/env python
"""
  dirac-rss-sync

    Script that synchronizes the resources described on the CS with the RSS.
    By default, it sets their Status to `Unknown`, StatusType to `all` and
    reason to `Synchronized`. However, it can copy over the status on the CS to
    the RSS. Important: If the StatusType is not defined on the CS, it will set
    it to Banned !

    Usage:
      dirac-rss-sync
        --init                Initialize the element to the status in the CS ( applicable for StorageElements )
        --element=            Element family to be Synchronized ( Site, Resource or Node ) or `all`


    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from datetime import datetime, timedelta
from DIRAC import version, gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Base  import Script

__RCSID__  = '$Id$'

subLogger  = None
switchDict = {}

DEFAULT_STATUS = 'Banned'
#Add 24 hours to the datetime (it is going to be inserted in the "TokenExpiration" Column of "SiteStatus")
Datetime       = datetime.utcnow() + timedelta(hours=24)

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = (
    ( 'init',     'Initialize the element to the status in the CS ( applicable for StorageElements )' ),
    ( 'element=', 'Element family to be Synchronized ( Site, Resource or Node ) or `all`' ),
             )

  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''

  hLine = '  ' + '='*78 + '\n'

  usageMessage = hLine
  usageMessage += '  DIRAC %s\n' % version
  usageMessage += __doc__
  usageMessage += '\n' + hLine

  Script.setUsageMessage( usageMessage )

def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  switches = dict( Script.getUnprocessedSwitches() )

  # Default values
  switches.setdefault( 'element', None )
  if not switches[ 'element' ] in ( 'all', 'Site', 'Resource', 'Node', None ):
    subLogger.error( "Found %s as element switch" % switches[ 'element' ] )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return switches

#Script initialization
subLogger  = gLogger.getSubLogger( __file__ )
registerSwitches()
registerUsageMessage()
switchDict = parseSwitches()

#############################################################################
# We can define the script body now

from DIRAC.WorkloadManagementSystem.Client.ServerUtils import jobDB
from DIRAC                                             import gConfig
from DIRAC.ResourceStatusSystem.Utilities              import Synchronizer, CSHelpers, RssConfiguration
from DIRAC.ResourceStatusSystem.Client                 import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem           import StateMachine

def synchronize():
  '''
    Given the element switch, adds rows to the <element>Status tables with Status
    `Unknown` and Reason `Synchronized`.
  '''

  synchronizer = Synchronizer.Synchronizer()

  if switchDict[ 'element' ] in ( 'Site', 'all' ):
    subLogger.info( 'Synchronizing Sites' )
    res = synchronizer._syncSites()
    if not res[ 'OK' ]:
      return res

  if switchDict[ 'element' ] in ( 'Resource', 'all' ):
    subLogger.info( 'Synchronizing Resource' )
    res = synchronizer._syncResources()
    if not res[ 'OK' ]:
      return res

  if switchDict[ 'element' ] in ( 'Node', 'all' ):
    subLogger.info( 'Synchronizing Nodes' )
    res = synchronizer._syncNodes()
    if not res[ 'OK' ]:
      return res

  return S_OK()

def initSites():
  '''
    Initializes Sites statuses taking their values from the "SiteMask" table of "JobDB" database.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  sites = jobDB.getAllSiteMaskStatus()

  if not sites[ 'OK' ]:
    subLogger.error( sites[ 'Message' ] )
    DIRACExit( 1 )

  for site, elements in sites['Value'].iteritems():
    table  = { 'table': 'SiteStatus' }
    parameters = { 'status': elements[0],
                   'reason': 'Synchronized',
                   'name': site,
                   'dateEffective': elements[1],
                   'tokenExpiration': Datetime,
                   'elementType': 'Site',
                   'statusType': 'all',
                   'lastCheckTime': None,
                   'tokenOwner': elements[2],
                   'meta': table }

    result = rssClient.addIfNotThereStatusElement( "Site", "Status", **parameters )

    if not result[ 'OK' ]:
      subLogger.error( result[ 'Message' ] )
      DIRACExit( 1 )

  return S_OK()

def initSEs():
  '''
    Initializes SEs statuses taking their values from the CS.
  '''

  #WarmUp local copy
  CSHelpers.warmUp()

  subLogger.info( 'Initializing SEs' )

  rssClient = ResourceStatusClient.ResourceStatusClient()

  ses = CSHelpers.getStorageElements()
  if not ses[ 'OK' ]:
    return ses
  ses = ses[ 'Value' ]

  statuses    = StateMachine.RSSMachine( None ).getStates()
  statusTypes = RssConfiguration.RssConfiguration().getConfigStatusType( 'StorageElement' )
  reason      = 'dirac-rss-sync'

  subLogger.debug( statuses )
  subLogger.debug( statusTypes )

  for se in ses:

    subLogger.debug( se )

    opts = gConfig.getOptionsDict( '/Resources/StorageElements/%s' % se )
    if not opts[ 'OK' ]:
      subLogger.warn( opts[ 'Message' ] )
      continue
    opts = opts[ 'Value' ]

    subLogger.debug( opts )

    # We copy the list into a new object to remove items INSIDE the loop !
    statusTypesList = statusTypes[:]

    for statusType, status in opts.iteritems():

      #Sanity check...
      if not statusType in statusTypesList:
        continue

      #Transforms statuses to RSS terms
      if status in ( 'NotAllowed', 'InActive' ):
        status = 'Banned'

      if not status in statuses:
        subLogger.error( '%s not a valid status for %s - %s' % ( status, se, statusType ) )
        continue

      # We remove from the backtracking
      statusTypesList.remove( statusType )

      subLogger.debug( [ se,statusType,status,reason ] )
      result = rssClient.addOrModifyStatusElement( 'Resource', 'Status', name = se,
                                                   statusType = statusType, status = status,
                                                   elementType = 'StorageElement',
                                                   reason = reason )

      if not result[ 'OK' ]:
        subLogger.error( 'Failed to modify' )
        subLogger.error( result[ 'Message' ] )
        continue

    #Backtracking: statusTypes not present on CS
    for statusType in statusTypesList:

      result = rssClient.addOrModifyStatusElement( 'Resource', 'Status', name = se,
                                                   statusType = statusType, status = DEFAULT_STATUS,
                                                   elementType = 'StorageElement',
                                                   reason = reason )
      if not result[ 'OK' ]:
        subLogger.error( 'Error in backtracking for %s,%s,%s' % ( se, statusType, status ) )
        subLogger.error( result[ 'Message' ] )

  return S_OK()

#...............................................................................

def run():
  '''
    Main function of the script
  '''

  result = synchronize()
  if not result[ 'OK' ]:
    subLogger.error( result[ 'Message' ] )
    DIRACExit( 1 )

  if 'init' in switchDict:

    result = initSites()
    if not result[ 'OK' ]:
      subLogger.error( result[ 'Message' ] )
      DIRACExit( 1 )

    result = initSEs()
    if not result[ 'OK' ]:
      subLogger.error( result[ 'Message' ] )
      DIRACExit( 1 )

#...............................................................................

if __name__ == "__main__":

  #Run script
  run()

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
