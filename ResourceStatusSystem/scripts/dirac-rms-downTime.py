#!/usr/bin/env python
"""
  dirac-rms-downTime

    Add/Remove/Modify a new DownTime for a given Site or Service.

    Usage:
      dirac-rms-downTime
        --downtimeID=         The ID of the downtime
        --element=            Element (Site, Service) affected by the downtime
        --name=               Name of the element
        --startDate=          Starting date of the downtime
        --endDate=            Ending date of the downtime
        --severity=           Severity of the downtime (Warning, Outage)
        --description=        Description of the downtime
        --link=               URL of the downtime announcement
        --dateEffective=      Date of downtime announcement
        --query=              A valid query type (select, add, delete)


    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                           import Script
from DIRAC.ResourceStatusSystem.Client         import ResourceManagementClient
import datetime


__RCSID__ = '$Id:$'

subLogger = None
switchDict = {}

def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = ( 
    ( 'downtimeID=', 'ID of the downtime' ),
    ( 'element=', 'Element (Site, Service) affected by the downtime' ),
    ( 'name=', 'Name of the element' ),
    ( 'startDate=', 'Starting date of the downtime' ),
    ( 'endDate=', 'Ending date of the downtime' ),
    ( 'severity=', 'Severity of the downtime (Warning, Outage)' ),
    ( 'description=', 'Description of the downtime' ),
    ( 'link=', 'URL of the downtime announcement' ),
    ( 'dateEffective=', 'Date of downtime announcement' )
             )

  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

  Script.registerSwitch( "q:", "query=", "A valid query type (select, add, delete)" )


def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''

  usageMessage = '  DIRAC version: %s' % version
  #usageMessage += __doc__

  Script.setUsageMessage( usageMessage )


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    error( "Found the following positional args '%s', but we only accept switches" % args )

  switches = dict( Script.getUnprocessedSwitches() )

  # Default values
  switches.setdefault( 'downtimeID', None )
  switches.setdefault( 'element', None )
  switches.setdefault( 'name', None )
  switches.setdefault( 'startDate', None )
  switches.setdefault( 'endDate', None )
  switches.setdefault( 'severity', None )
  switches.setdefault( 'description', None )
  switches.setdefault( 'link', None )
  switches.setdefault( 'dateEffective', None )
  switches.setdefault( 'query', None )
  switches.setdefault( 'q', None )

  if not 'query' in switches and not 'q' in switches:
    error( "query Switch is mandatory but found missing" )

  if not switches['query'] in ( 'select', 'add', 'delete' ) and \
     not switches['q'] in ( 'select', 'add', 'delete' ):
    argument = switches[ 'query' ] if switches[ 'query' ] else switches['q']
    error( "'%s' is an invalid argument for switch 'query'" % argument )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return switches

#...............................................................................

def select():
  '''
    Given the switches, request a query 'select' on the ResourceManagementDB
    that gets from DowntimeCache all rows that match the parameters given.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  meta = { 'columns' : [ 'downtimeID', 'element', 'name', 'startDate', 'endDate',
                         'severity', 'description', 'link', 'dateEffective' ] }

  output = rmsClient.selectDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                          element = switchDict[ 'element' ],
                                          name = switchDict[ 'name' ],
                                          startDate = switchDict[ 'startDate' ],
                                          endDate = switchDict[ 'endDate' ],
                                          severity = switchDict[ 'severity' ],
                                          description = switchDict[ 'description' ],
                                          link = switchDict[ 'link' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          meta = meta )

  return output


def add():
  '''
    Given the switches, request a query 'addOrModify' on the ResourceManagementDB
    that inserts or updates-if-duplicated from DowntimeCache.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  meta = { 'columns' : [ 'downtimeID', 'element', 'name', 'startDate', 'endDate',
                         'severity', 'description', 'link', 'dateEffective' ] }

  output = rmsClient.addOrModifyDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                               element = switchDict[ 'element' ],
                                               name = switchDict[ 'name' ],
                                               startDate = switchDict[ 'startDate' ],
                                               endDate = switchDict[ 'endDate' ],
                                               severity = switchDict[ 'severity' ],
                                               description = switchDict[ 'description' ],
                                               link = switchDict[ 'link' ],
                                               dateEffective = switchDict[ 'dateEffective' ],
                                               meta = meta )

  return output


def delete():
  '''
    Given the switches, request a query 'delete' on the ResourceManagementDB
    that deletes from DowntimeCache all rows that match the parameters given.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  meta = { 'columns' : [ 'downtimeID', 'element', 'name', 'startDate', 'endDate',
                         'severity', 'description', 'link', 'dateEffective' ] }


  output = rmsClient.deleteDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                          element = switchDict[ 'element' ],
                                          name = switchDict[ 'name' ],
                                          startDate = switchDict[ 'startDate' ],
                                          endDate = switchDict[ 'endDate' ],
                                          severity = switchDict[ 'severity' ],
                                          description = switchDict[ 'description' ],
                                          link = switchDict[ 'link' ],
                                          dateEffective = switchDict[ 'dateEffective' ]
                                        )

  return output

#...............................................................................

def error( msg ):
  '''
    Format error messages
  '''
  subLogger.error( "\nERROR:" )
  subLogger.error( "\t" + msg )
  subLogger.error( "\tPlease, check documentation below" )
  Script.showHelp()
  DIRACExit( 1 )


def confirm( query, matches ):
  if type( matches ) == long:
    subLogger.notice( "\nNOTICE: '%s' query was successful ( match number: %s )! \n" % ( query, matches ) )
  else:
    subLogger.notice( "\nNOTICE: '%s' query was successful ( match number: %d )! \n" % ( query, len( matches ) ) )


def printTable( table, columns ):
  '''
    Prints query output on a tabular
  '''

  #columns = tuple( map( lambda x: x.upper(), columns ) )
  table = list( table )
  table.insert( 0, columns )

  columns_width = []

  for i in zip( *table ):
    columns_width.append( max( [ len( str( x ) ) for x in i ] ) )

  columns_separator = True

  for row in table:
    rowline = "| " + " | ".join( 
                   "{:{}}".format( row[i].strftime( '%Y-%m-%d %H:%M:%S' ), item ) if type( row[i] ) == datetime.datetime
                   else "{:{}}".format( row[i], item )
                   for i, item in enumerate( columns_width )
                   ) + " |"

    if columns_separator:
      subLogger.notice( "-" * len( rowline ) )

    subLogger.notice( rowline )

    if columns_separator:
      subLogger.notice( "-" * len( rowline ) )
      columns_separator = False

  subLogger.notice( "-" * len( rowline ) )

#...............................................................................

def run( switchDict ):
  '''
    Main function of the script
  '''

  query = switchDict[ 'q' ] if switchDict['q'] else switchDict['query']
  output = None

  # exectue the query request: e.g. if it's a 'select' it executes 'select()'
  # the same if it is add, delete
  output = eval( query + '()' )

  if not output[ 'OK' ]:
    error( output[ 'Message' ] )

  table = output[ 'Value' ]

  if 'Columns' in output and len( table ) != 0:
    printTable( table, output[ 'Columns' ] )
  confirm( query, output[ 'Value' ] )


#...............................................................................

if __name__ == "__main__":

  subLogger = gLogger.getSubLogger( __file__ )

  #Script initialization
  registerSwitches()
  registerUsageMessage()
  switchDict = parseSwitches()

  #Run script
  run( switchDict )

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
