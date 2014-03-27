#!/usr/bin/env python
"""
  dirac-rss-db

    Script that dumps the DB information for the elements into the standard output.
    If returns information concerning the StatusType and Status attributes.

    Usage:
      dirac-rss-db
        --element=            Element family to be Synchronized ( Site, Resource or Node )
        --tableType=          A valid table type (Status, Log, History)
        --name=               ElementName; None if default
        --statusType=         StatusType; None if default
        --status=             Status; None if default
        --elementType=        ElementType narrows the search (string, list); None if default
        --reason=             Decision that triggered the assigned status
        --dateEffective=      Time-stamp of downtime announcement
        --lastCheckTime=      Time-stamp setting last time the status & status were checked
        --tokenOwner=         Owner of the token; None if default
        --tokenExpiration=    Time-stamp setting validity of token ownership
        --query=              A valid query type (select, insert, update, add, modify, delete)


    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                           import Script
from DIRAC.ResourceStatusSystem.Client         import ResourceStatusClient
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
    ( 'element=', 'Element family to be Synchronized ( Site, Resource, Node )' ),
    ( 'tableType=', 'A valid table type (Status, Log, History)' ),
    ( 'name=', 'ElementName; None if default' ),
    ( 'statusType=', 'StatusType; None if default' ),
    ( 'status=', 'Status; None if default' ),
    ( 'elementType=', 'ElementType narrows the search; None if default' ),
    ( 'reason=', 'Decision that triggered the assigned status' ),
    ( 'dateEffective=', 'Time-stamp of downtime announcement' ),
    ( 'lastCheckTime=', 'Time-stamp setting last time the status & status were checked' ),
    ( 'tokenOwner=', 'Owner of the token; None if default' ),
    ( 'tokenExpiration=', 'Time-stamp setting validity of token ownership' )
             )

  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )

  Script.registerSwitch( "q:", "query=", "A valid query type (select, insert, update, add, modify, delete)" )


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
  switches.setdefault( 'name', None )
  switches.setdefault( 'statusType', None )
  switches.setdefault( 'status', None )
  switches.setdefault( 'elementType', None )
  switches.setdefault( 'reason', None )
  switches.setdefault( 'dateEffective', None )
  switches.setdefault( 'lastCheckTime', None )
  switches.setdefault( 'tokenOwner', None )
  switches.setdefault( 'tokenExpiration', None )
  switches.setdefault( 'query', None )
  switches.setdefault( 'q', None )


  if not 'element' in switches:
    error( "element Switch is mandatory but found missing" )

  if not switches[ 'element' ] in ( 'Site', 'Resource', 'Node' ):
    error( "'%s' is an invalid argument for switch 'element'" % switches[ 'element' ] )

  if not 'tableType' in switches:
    error( "tableType Switch is mandatory but found missing" )

  if not switches[ 'tableType' ] in ( 'Status', 'Log', 'History' ):
    error( "'%s' is an invalid argument for switch 'tableType'" % switches[ 'tableType' ] )

  if not 'query' in switches and not 'q' in switches:
    error( "query Switch is mandatory but found missing" )

  if not switches['query'] in ( 'select', 'insert', 'update', 'add', 'modify', 'delete' ) and \
     not switches['q'] in ( 'select', 'insert', 'update', 'add', 'modify', 'delete' ):
    argument = switches[ 'query' ] if switches[ 'query' ] else switches['q']
    error( "'%s' is an invalid argument for switch 'query'" % argument )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return switches

#...............................................................................

def select():
  '''
    Given the switches, request a query 'select' on the ResourceStatusDB
    that gets from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }

  output = rssClient.selectStatusElement( element = switchDict[ 'element' ],
                                          tableType = switchDict[ 'tableType' ],
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                          meta = meta )

  return output


def insert():
  '''
    Given the switches, request a query 'insert' on the ResourceStatusDB
    that inserts on <element><tableType> a new row with the arguments given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }

  output = rssClient.insertStatusElement( element = switchDict[ 'element' ],
                                          tableType = switchDict[ 'tableType' ],
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                        )

  return output


def update():
  '''
    Given the switches, request a query 'update' on the ResourceStatusDB
    that updates from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }

  output = rssClient.updateStatusElement( element = switchDict[ 'element' ],
                                          tableType = switchDict[ 'tableType' ],
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                        )

  return output


def add():
  '''
    Given the switches, request a query 'addOrModify' on the ResourceStatusDB
    that inserts or updates-if-duplicated from <element><tableType> and also adds
    a log if flag is active.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }

  output = rssClient.addOrModifyStatusElement( element = switchDict[ 'element' ],
                                               tableType = switchDict[ 'tableType' ],
                                               name = switchDict[ 'name' ],
                                               statusType = switchDict[ 'statusType' ],
                                               status = switchDict[ 'status' ],
                                               elementType = switchDict[ 'elementType' ],
                                               reason = switchDict[ 'reason' ],
                                               dateEffective = switchDict[ 'dateEffective' ],
                                               lastCheckTime = switchDict[ 'lastCheckTime' ],
                                               tokenOwner = switchDict[ 'tokenOwner' ],
                                               tokenExpiration = switchDict[ 'tokenExpiration' ],
                                               meta = meta )

  return output


def modify():
  '''
    Given the switches, request a query 'modify' on the ResourceStatusDB
    that updates from <element><tableType> and also adds a log if flag is active.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }

  output = rssClient.modifyStatusElement( element = switchDict[ 'element' ],
                                          tableType = switchDict[ 'tableType' ],
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                          meta = meta )

  return output


def delete():
  '''
    Given the switches, request a query 'delete' on the ResourceStatusDB
    that deletes from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }


  output = rssClient.deleteStatusElement( element = switchDict[ 'element' ],
                                          tableType = switchDict[ 'tableType' ],
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
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

  query = switchDict[ 'q' ] if switchDict[ 'q' ] else switchDict['query']
  output = None

  # exectue the query request: e.g. if it's a 'select' it executes 'select()'
  # the same if it is insert, update, add, modify, delete
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
