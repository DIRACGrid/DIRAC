#!/usr/bin/env python
"""
  dirac-rss-query-db

    Script that dumps the DB information for the elements into the standard output.
    If returns information concerning the StatusType and Status attributes.

    Usage:
        dirac-rss-query-db [option] <query> <element> <tableType>

    Queries:
<<<<<<< HEAD
        [select|insert|update|add|modify|delete]
=======
        [select|add|modify|delete]
>>>>>>> rel-v6r11

    Elements:
        [site|resource|component|node]

    TableTypes:
        [status|log|history]

    Options:
        --name=               ElementName (it admits a comma-separated list of element names); None by default
<<<<<<< HEAD
        --statusType=         A valid StatusType argument (it admits a comma-separated list of statusTypes 
                              e.g. ReadAccess, WriteAccess, RemoveAccess ); None by default
        --status=             A valid Status argument ( active, probing, degraded, banned, unknown, error );
=======
        --statusType=         A valid StatusType argument (it admits a comma-separated list of statusTypes
                              e.g. ReadAccess, WriteAccess, RemoveAccess ); None by default
        --status=             A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error );
>>>>>>> rel-v6r11
                              None by default
        --elementType=        ElementType narrows the search (string, list); None by default
        --reason=             Decision that triggered the assigned status
        --lastCheckTime=      Time-stamp setting last time the status & status were checked
<<<<<<< HEAD
        --tokenOwner=         Owner of the token; None by default
        --tokenExpiration=    Time-stamp setting validity of token ownership
=======
        --tokenOwner=         Owner of the token ( to specify only with select/delete queries )
>>>>>>> rel-v6r11

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                                  import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                                        import Script
from DIRAC.ResourceStatusSystem.Client                      import ResourceStatusClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
<<<<<<< HEAD
=======
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
>>>>>>> rel-v6r11
from DIRAC.Core.Utilities                                   import Time
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
    ( 'statusType=', 'A valid StatusType argument (it admits a comma-separated list of statusTypes); None if default' ),
<<<<<<< HEAD
    ( 'status=', 'A valid Status argument ( active, probing, degraded, banned, unknown, error ); None if default' ),
=======
    ( 'status=', 'A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error ); None if default' ),
>>>>>>> rel-v6r11
    ( 'elementType=', 'ElementType narrows the search; None if default' ),
    ( 'reason=', 'Decision that triggered the assigned status' ),
    ( 'lastCheckTime=', 'Time-stamp setting last time the status & status were checked' ),
    ( 'tokenOwner=', 'Owner of the token ( to specify only with select/delete queries )' ),
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
  if len( args ) < 3:
    error( "Missing all mandatory 'query', 'element', 'tableType' arguments" )
<<<<<<< HEAD
  elif not args[0].lower() in ( 'select', 'insert', 'update', 'add', 'modify', 'delete' ):
=======
  elif not args[0].lower() in ( 'select', 'add', 'modify', 'delete' ):
>>>>>>> rel-v6r11
    error( "Incorrect 'query' argument" )
  elif not args[1].lower() in ( 'site', 'resource', 'component', 'node' ):
    error( "Incorrect 'element' argument" )
  elif not args[2].lower() in ( 'status', 'log', 'history' ):
    error( "Incorrect 'tableType' argument" )
  else:
    query = args[0].lower()

  switches = dict( Script.getUnprocessedSwitches() )

  # Default values
  switches.setdefault( 'name', None )
  switches.setdefault( 'statusType', None )
  switches.setdefault( 'status', None )
  switches.setdefault( 'elementType', None )
  switches.setdefault( 'reason', None )
  switches.setdefault( 'lastCheckTime', None )
  switches.setdefault( 'tokenOwner', None )
<<<<<<< HEAD
  switches.setdefault( 'tokenExpiration', None )
=======
>>>>>>> rel-v6r11

  if 'status' in switches and switches[ 'status' ] is not None:
    switches[ 'status' ] = switches[ 'status' ].title()
    if not switches[ 'status' ] in ( 'Active', 'Probing', 'Degraded', 'Banned', 'Unknown', 'Error' ):
<<<<<<< HEAD
      error("'%s' is an invalid argument for switch 'status'" % switches[ 'status' ] )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  return args, switches
  
=======
      error( "'%s' is an invalid argument for switch 'status'" % switches[ 'status' ] )

  # when it's a add/modify query and status/reason/statusType are not specified 
  #then some specific defaults are set up
  if query == 'add' or query == 'modify':
    if not 'status' in switches or switches[ 'status' ] is None:
      switches[ 'status' ] = 'Unknown'
    if not 'reason' in switches or switches[ 'reason' ] is None:
      switches[ 'reason' ] = 'Unknown reason'
    if not 'statusType' in switches or switches[ 'statusType' ] is None:
      switches[ 'statusType' ] = 'all'


  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )
>>>>>>> rel-v6r11
  
  return args, switches


#...............................................................................
# UTILS: to check and unpack
<<<<<<< HEAD
=======

def getToken( key ):
  '''
    Function that gets the userName from the proxy
  '''

  proxyInfo = getProxyInfo()
  if not proxyInfo[ 'OK' ]:
    error( str( proxyInfo ) )

  if key.lower() == 'owner':
    userName = proxyInfo[ 'Value' ][ 'username' ]  
    tokenOwner = S_OK( userName )
    if not tokenOwner[ 'OK' ]:
      error( tokenOwner[ 'Message' ] )
    return tokenOwner[ 'Value' ]

  elif key.lower() == 'expiration':
    expiration = proxyInfo[ 'Value' ][ 'secondsLeft' ]
    tokenExpiration = S_OK( expiration )  
    if not tokenExpiration[ 'OK' ]:
      error( tokenExpiration[ 'Message' ] )

    now = Time.dateTime()   
    #datetime.datetime.utcnow()
    expirationDate = now + datetime.timedelta( seconds=tokenExpiration['Value'] )
    expirationDate = Time.toString( expirationDate )
    expirationDate = expirationDate.split('.')[0]
    return expirationDate 

>>>>>>> rel-v6r11

def checkStatusTypes( statusTypes ):
  '''
    To check if values for 'statusType' are valid
  '''

  opsH = Operations().getValue( 'ResourceStatus/Config/StatusTypes/StorageElement' )
  acceptableStatusTypes = opsH.replace( ',', '' ).split()
<<<<<<< HEAD
  acceptableStatusTypes.append( 'all' )
  
  for statusType in statusTypes:
    if not statusType in acceptableStatusTypes and statusType != 'all':
      error( "'%s' is a wrong value for switch 'statusType'.\n\tThe acceptable values are:\n\t%s" 
             % ( statusType, str(acceptableStatusTypes) ) )
=======

  for statusType in statusTypes:
    if not statusType in acceptableStatusTypes and statusType != 'all':
      acceptableStatusTypes.append('all')
      error( "'%s' is a wrong value for switch 'statusType'.\n\tThe acceptable values are:\n\t%s"
             % ( statusType, str( acceptableStatusTypes ) ) )


  if 'all' in statusType:
    return acceptableStatusTypes
  else:
    return statusTypes
>>>>>>> rel-v6r11


def unpack( switchDict ):
  '''
    To split and process comma-separated list of values for 'name' and 'statusType'
  '''

  switchDictSet = []
  names = []
  statusTypes = []

  if switchDict[ 'name' ] is not None:
    names = filter( None, switchDict[ 'name' ].split( ',' ) )

  if switchDict[ 'statusType' ] is not None:
    statusTypes = filter( None, switchDict[ 'statusType' ].split( ',' ) )
    statusTypes = checkStatusTypes( statusTypes )


  if len( names ) > 0 and len( statusTypes ) > 0:
    combinations = [ ( a, b ) for a in names for b in statusTypes ]
    for combination in combinations:
      n, s = combination
      switchDictClone = switchDict.copy()
      switchDictClone[ 'name' ] = n
      switchDictClone[ 'statusType' ] = s
      switchDictSet.append( switchDictClone )
  elif len( names ) > 0 and len( statusTypes ) == 0:
    for name in names:
      switchDictClone = switchDict.copy()
      switchDictClone[ 'name' ] = name
      switchDictSet.append( switchDictClone )
  elif len( names ) == 0 and len( statusTypes ) > 0:
    for statusType in statusTypes:
      switchDictClone = switchDict.copy()
      switchDictClone[ 'statusType' ] = statusType
      switchDictSet.append( switchDictClone )
  elif len( names ) == 0 and len( statusTypes ) == 0:
    switchDictClone = switchDict.copy()
    switchDictClone[ 'name' ] = None
    switchDictClone[ 'statusType' ] = None
    switchDictSet.append( switchDictClone )

  return switchDictSet



<<<<<<< HEAD
#...............................................................................
# UTILS: for filtering 'select' output

def filterReason( selectOutput, reason ):
  '''
    Selects all the elements that match 'reason'
  '''

  elements = selectOutput
  elementsFiltered = []
  if reason is not None:
    for e in elements:
      print reason, e[ 'reason' ]
      if reason in e[ 'reason' ]:
        elementsFiltered.append( e )
  else:
    elementsFiltered = elements

  return elementsFiltered


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


=======
>>>>>>> rel-v6r11
#...............................................................................
# UTILS: for filtering 'select' output

<<<<<<< HEAD
def select( args, switchDict ):
=======
def filterReason( selectOutput, reason ):
>>>>>>> rel-v6r11
  '''
    Selects all the elements that match 'reason'
  '''

  elements = selectOutput
  elementsFiltered = []
  if reason is not None:
    for e in elements:
      if reason in e[ 'reason' ]:
        elementsFiltered.append( e )
  else:
    elementsFiltered = elements

  return elementsFiltered

<<<<<<< HEAD
  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.selectStatusElement( element = args[1].title(),
                                          tableType = args[2].title(),
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          #reason = switchDict[ 'reason' ],
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                          meta = meta )
  result['output'] = [ dict( zip( output[ 'Columns' ], e ) ) for e in output[ 'Value' ] ]
  result['output'] = filterReason( result['output'], switchDict[ 'reason' ] )
  result['match'] = len( result['output'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


def insert( args, switchDict ):
=======

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
>>>>>>> rel-v6r11
  '''
    Format confirmation messages
  '''

  subLogger.notice( "\nNOTICE: '%s' request successfully executed ( matches' number: %s )! \n" % ( query, matches ) )

<<<<<<< HEAD
  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.insertStatusElement( element = args[1].title(),
                                          tableType = args[2].title(),
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

  result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result
=======
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
>>>>>>> rel-v6r11

  subLogger.notice( line )

<<<<<<< HEAD
def update( args, switchDict ):
=======

#...............................................................................

def select( args, switchDict ):
>>>>>>> rel-v6r11
  '''
    Given the switches, request a query 'select' on the ResourceStatusDB
    that gets from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

<<<<<<< HEAD
  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.updateStatusElement( element = args[1].title(),
=======
  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }
   
  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.selectStatusElement( element = args[1].title(),
>>>>>>> rel-v6r11
                                          tableType = args[2].title(),
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          #reason = switchDict[ 'reason' ],
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          #tokenExpiration = switchDict[ 'tokenExpiration' ],
                                          meta = meta )
  result['output'] = [ dict( zip( output[ 'Columns' ], e ) ) for e in output[ 'Value' ] ]
  result['output'] = filterReason( result['output'], switchDict[ 'reason' ] )
  result['match'] = len( result['output'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

<<<<<<< HEAD
  result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

=======
>>>>>>> rel-v6r11
  return result


def add( args, switchDict ):
  '''
    Given the switches, request a query 'addOrModify' on the ResourceStatusDB
    that inserts or updates-if-duplicated from <element><tableType> and also adds
    a log if flag is active.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.addOrModifyStatusElement( element = args[1].title(),
                                               tableType = args[2].title(),
                                               name = switchDict[ 'name' ],
                                               statusType = switchDict[ 'statusType' ],
                                               status = switchDict[ 'status' ],
                                               elementType = switchDict[ 'elementType' ],
                                               reason = switchDict[ 'reason' ],
<<<<<<< HEAD
                                               dateEffective = switchDict[ 'dateEffective' ],
                                               lastCheckTime = switchDict[ 'lastCheckTime' ],
                                               tokenOwner = switchDict[ 'tokenOwner' ],
                                               tokenExpiration = switchDict[ 'tokenExpiration' ],
                                              )

  result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result
=======
                                               #dateEffective = switchDict[ 'dateEffective' ],
                                               #lastCheckTime = switchDict[ 'lastCheckTime' ],
                                               tokenOwner = getToken( 'owner' ),
                                               tokenExpiration = getToken( 'expiration' )
                                              )

  if 'Value' in output:
    result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None
>>>>>>> rel-v6r11

  return result

<<<<<<< HEAD
=======

>>>>>>> rel-v6r11
def modify( args, switchDict ):
  '''
    Given the switches, request a query 'modify' on the ResourceStatusDB
    that updates from <element><tableType> and also adds a log if flag is active.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
  output = rssClient.modifyStatusElement( element = args[1].title(),
                                          tableType = args[2].title(),
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
<<<<<<< HEAD
                                          dateEffective = switchDict[ 'dateEffective' ],
                                          lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          tokenExpiration = switchDict[ 'tokenExpiration' ],
                                         )

  result['match'] = int( output['Value'] )
=======
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          #lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = getToken( 'owner' ),
                                          tokenExpiration = getToken( 'expiration' )
                                         )

  if 'Value' in output:
    result['match'] = int( output['Value'] )
>>>>>>> rel-v6r11
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


def delete( args, switchDict ):
  '''
    Given the switches, request a query 'delete' on the ResourceStatusDB
    that deletes from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()


<<<<<<< HEAD
=======
  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
>>>>>>> rel-v6r11
  output = rssClient.deleteStatusElement( element = args[1].title(),
                                          tableType = args[2].title(),
                                          name = switchDict[ 'name' ],
                                          statusType = switchDict[ 'statusType' ],
                                          status = switchDict[ 'status' ],
                                          elementType = switchDict[ 'elementType' ],
                                          reason = switchDict[ 'reason' ],
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          #lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = switchDict[ 'tokenOwner' ],
                                          #tokenExpiration = switchDict[ 'tokenExpiration' ]
                                        )

<<<<<<< HEAD
  result['match'] = int( output['Value'] )
=======
  if 'Value' in output:
    result['match'] = int( output['Value'] )
>>>>>>> rel-v6r11
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


#...............................................................................

<<<<<<< HEAD
def run( args, switchDict ):
  '''
    Main function of the script
  '''
   
  query = args[0]

  # exectue the query request: e.g. if it's a 'select' it executes 'select()'
  # the same if it is insert, update, add, modify, delete
  result = eval( query + '( args, switchDict )' )

  if result[ 'successful' ]:
    if query == 'select' and result['match'] > 0:
      printTable( result[ 'output' ] )
    confirm( query, result['match'] )
  else:
    error( result[ 'message' ] )
=======
def run( args, switchDictSet ):
  '''
    Main function of the script
  '''

  query = args[0]

  matches = 0
  table = []
  
  for switchDict in switchDictSet:
    # exectue the query request: e.g. if it's a 'select' it executes 'select()'
    # the same if it is insert, update, add, modify, delete
    result = eval( query + '( args, switchDict )' )

    if result[ 'successful' ]:
      if query == 'select' and result['match'] > 0:
        table.extend( result[ 'output' ] )
      matches = matches + result[ 'match' ]
    else:
      error( result[ 'message' ] )

  if query == 'select' and matches > 0:
    printTable( table )
  confirm( query, matches )
>>>>>>> rel-v6r11

#...............................................................................

if __name__ == "__main__":

  subLogger = gLogger.getSubLogger( __file__ )

  #Script initialization
  registerSwitches()
  registerUsageMessage()
  args, switchDict = parseSwitches()

  #Unpack switchDict if 'name' or 'statusType' have multiple values
  switchDictSet = unpack( switchDict )

  #Run script
<<<<<<< HEAD
  for switchDict in switchDictSet:
    run( args, switchDict )
=======
  run( args, switchDictSet )
>>>>>>> rel-v6r11

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
