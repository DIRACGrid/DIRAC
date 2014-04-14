#!/usr/bin/env python
"""
  dirac-rss-query-dtcache

    Select/Add/Delete a new DownTime entry for a given Site or Service.

    Usage:
        dirac-rss-query-dtcache [option] <query>

    Queries:
        [select|add|delete]

    Options:
        --downtimeID=         The ID of the downtime
        --element=            Element (Site, Service) affected by the downtime
        --name=               Name of the element
        --startDate=          Starting date of the downtime
        --endDate=            Ending date of the downtime
        --severity=           Severity of the downtime (Warning, Outage)
        --description=        Description of the downtime
        --link=               URL of the downtime announcement
        --ongoing             To force "select" to return the ongoing downtimes

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                           import Script
from DIRAC.ResourceStatusSystem.Client         import ResourceManagementClient
from DIRAC.Core.Utilities                      import Time
import re
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
    ( 'ongoing', 'To force "select" to return the ongoing downtimes' )
             )

  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )


def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''

  usageMessage = 'DIRAC version: %s \n' % version
  usageMessage += __doc__

  Script.setUsageMessage( usageMessage )


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if len( args ) == 0:
    error( "Missing mandatory 'query' argument" )
  elif not args[0].lower() in ( 'select', 'add', 'delete' ):
    error( "Missing mandatory argument" )
  else:
    query = args[0].lower()

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

  if query in ( 'add', 'delete' ) and switches['downtimeID'] is None:
    error( "'downtimeID' switch is mandatory for '%s' but found missing" % query )

  if query in ( 'add', 'delete' ) and 'ongoing' in switches:
    error( "'ongoing' switch can be used only with 'select'" )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return ( args, switches )


#...............................................................................
# UTILS: for filtering 'select' output

def filterDate( selectOutput, start, end ):
  '''
    Selects all the downtimes that meet the constraints of 'start' and 'end' dates
  '''

  downtimes = selectOutput
  downtimesFiltered = []

  if start is not None:
    try:
      start = Time.fromString( start )
    except:
      error( "datetime formt is incorrect, pls try [%Y-%m-%d[ %H:%M:%S]]" )
    start = Time.toEpoch( start )

  if end is not None:
    try:
      end = Time.fromString( end )
    except:
      error( "datetime formt is incorrect, pls try [%Y-%m-%d[ %H:%M:%S]]" )
    end = Time.toEpoch( end )

  if start is not None and end is not None:
    for dt in downtimes:
      dtStart = Time.toEpoch( dt[ 'startDate' ] )
      dtEnd = Time.toEpoch( dt[ 'endDate' ] )
      if ( dtStart >= start ) and ( dtEnd <= end ):
        downtimesFiltered.append( dt )

  elif start is not None and end is None:
    for dt in downtimes:
      dtStart = Time.toEpoch( dt[ 'startDate' ] )
      if dtStart >= start:
        downtimesFiltered.append( dt )

  elif start is None and end is not None:
    for dt in downtimes:
      dtEnd = Time.toEpoch( dt[ 'endDate' ] )
      if dtEnd <= end:
        downtimesFiltered.append( dt )

  else:
    downtimesFiltered = downtimes

  return downtimesFiltered


def filterOngoing( selectOutput ):
  '''
    Selects all the ongoing downtimes
  '''

  downtimes = selectOutput
  downtimesFiltered = []
  currentDate = Time.toEpoch( Time.dateTime() )

  for dt in downtimes:
    dtStart = Time.toEpoch( dt[ 'startDate' ] )
    dtEnd = Time.toEpoch( dt[ 'endDate' ] )
    if ( dtStart <= currentDate ) and ( dtEnd >= currentDate ):
      downtimesFiltered.append( dt )

  return downtimesFiltered


def filterDescription( selectOutput, description ):
  '''
    Selects all the downtimes that match 'description'
  '''

  downtimes = selectOutput
  downtimesFiltered = []
  if description is not None:
    for dt in downtimes:
      if description in dt[ 'description' ]:
        downtimesFiltered.append( dt )
  else:
    downtimesFiltered = downtimes

  return downtimesFiltered

#...............................................................................
# Utils: for formatting query output and notifications

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
  '''
    Format confirmation messages
  '''

  subLogger.notice( "\nNOTICE: '%s' request successfully executed ( matches' number: %s )! \n" % ( query, matches ) )

def printTable( table ):
  '''
    Prints query output on a tabular
  '''

  columns_names = table[0].keys()
  columns = [ [c] for c in columns_names ]

  for row in table:
    for j, key in enumerate( row ):
      if type( row[key] ) == datetime.datetime:
        row[key] = Time.toString( row[key] )
      if row[key] is None:
        row[key] = ''
      columns[j].append( row[key] )

  columns_width = []
  for column in columns:
    columns_width.append( max( [ len( str( value ) ) for value in column ] ) )

  columns_separator = True
  for i in range( len( table ) + 1 ):
    row = ''
    for j in range( len( columns ) ):
      row = row + "{:{}}".format( columns[j][i], columns_width[j] ) + " | "
    row = "| " + row
    line = "-" * ( len( row ) - 1 )

    if columns_separator:
      subLogger.notice( line )

    subLogger.notice( row )

    if columns_separator:
      subLogger.notice( line )
      columns_separator = False

  subLogger.notice( line )



#...............................................................................


def select( switchDict ):
  '''
    Given the switches, request a query 'select' on the ResourceManagementDB
    that gets from DowntimeCache all rows that match the parameters given.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  meta = { 'columns' : [ 'downtimeID', 'element', 'name', 'startDate', 'endDate',
                         'severity', 'description', 'link', 'dateEffective' ] }

  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rmsClient.selectDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                          element = switchDict[ 'element' ],
                                          name = switchDict[ 'name' ],
                                          #startDate = switchDict[ 'startDate' ],
                                          #endDate = switchDict[ 'endDate' ],
                                          severity = switchDict[ 'severity' ],
                                          #description = switchDict[ 'description' ],
                                          #link = switchDict[ 'link' ],
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          meta = meta )

  result['output'] = [ dict( zip( output[ 'Columns' ], dt ) ) for dt in output[ 'Value' ] ]
  if 'ongoing' in switchDict:
    result['output'] = filterOngoing( result['output'] )
  else:
    result['output'] = filterDate( result['output'], switchDict[ 'startDate' ], switchDict[ 'endDate' ] )
  result['output'] = filterDescription( result['output'], switchDict[ 'description' ] )
  result['match'] = len( result['output'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


def add( switchDict ):
  '''
    Given the switches, request a query 'addOrModify' on the ResourceManagementDB
    that inserts or updates-if-duplicated from DowntimeCache.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rmsClient.addOrModifyDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                               element = switchDict[ 'element' ],
                                               name = switchDict[ 'name' ],
                                               startDate = switchDict[ 'startDate' ],
                                               endDate = switchDict[ 'endDate' ],
                                               severity = switchDict[ 'severity' ],
                                               description = switchDict[ 'description' ],
                                               link = switchDict[ 'link' ]
                                               #dateEffective = switchDict[ 'dateEffective' ]
                                              )

  result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


def delete( switchDict ):
  '''
    Given the switches, request a query 'delete' on the ResourceManagementDB
    that deletes from DowntimeCache all rows that match the parameters given.
  '''

  rmsClient = ResourceManagementClient.ResourceManagementClient()

  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rmsClient.deleteDowntimeCache( downtimeID = switchDict[ 'downtimeID' ],
                                          element = switchDict[ 'element' ],
                                          name = switchDict[ 'name' ],
                                          startDate = switchDict[ 'startDate' ],
                                          endDate = switchDict[ 'endDate' ],
                                          severity = switchDict[ 'severity' ],
                                          description = switchDict[ 'description' ],
                                          link = switchDict[ 'link' ]
                                          #dateEffective = switchDict[ 'dateEffective' ]
                                        )
  result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


#...............................................................................

def run( args, switchDict ):
  '''
    Main function of the script
  '''

  query = args[0]

  # it exectues the query request: e.g. if it's a 'select' it executes 'select()'
  # the same if it is add, delete
  result = eval( query + '( switchDict )' )

  if result[ 'successful' ]:
    if query == 'select' and result['match'] > 0:
      printTable( result[ 'output' ] )
    confirm( query, result['match'] )
  else:
    error( result[ 'message' ] )



#...............................................................................

if __name__ == "__main__":

  subLogger = gLogger.getSubLogger( __file__ )

  #Script initialization
  registerSwitches()
  registerUsageMessage()
  args, switchDict = parseSwitches()

  #Run script
  run( args, switchDict )

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
