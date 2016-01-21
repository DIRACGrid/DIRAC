#!/usr/bin/env python
"""
  dirac-rss-query-db

    Script that dumps the DB information for the elements into the standard output.
    If returns information concerning the StatusType and Status attributes.

    Usage:
        dirac-rss-query-db [option] <query> <element> <tableType>

    Queries:
        [select|add|modify|delete]

    Elements:
        [site|resource|component|node]

    TableTypes:
        [status|log|history]

    Options:
        --name=               ElementName (it admits a comma-separated list of element names); None by default
        --statusType=         A valid StatusType argument (it admits a comma-separated list of statusTypes
                              e.g. ReadAccess, WriteAccess, RemoveAccess ); None by default
        --status=             A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error );
                              None by default
        --elementType=        ElementType narrows the search (string, list); None by default
        --reason=             Decision that triggered the assigned status
        --lastCheckTime=      Time-stamp setting last time the status & status were checked
        --tokenOwner=         Owner of the token ( to specify only with select/delete queries )

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""

from DIRAC                                                  import gConfig, gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base                                        import Script
from DIRAC.ResourceStatusSystem.Client                      import ResourceStatusClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.Core.Utilities                                   import Time
from DIRAC.Core.Utilities.PrettyPrint                       import printTable 
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
    ( 'status=', 'A valid Status argument ( Active, Probing, Degraded, Banned, Unknown, Error ); None if default' ),
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
  elif not args[0].lower() in ( 'select', 'add', 'modify', 'delete' ):
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

  if 'status' in switches and switches[ 'status' ] is not None:
    switches[ 'status' ] = switches[ 'status' ].title()
    if not switches[ 'status' ] in ( 'Active', 'Probing', 'Degraded', 'Banned', 'Unknown', 'Error' ):
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
  
  return args, switches


#...............................................................................
# UTILS: to check and unpack

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


def checkStatusTypes( statusTypes ):
  '''
    To check if values for 'statusType' are valid
  '''

  opsH = Operations().getValue( 'ResourceStatus/Config/StatusTypes/StorageElement' )
  acceptableStatusTypes = opsH.replace( ',', '' ).split()

  for statusType in statusTypes:
    if not statusType in acceptableStatusTypes and statusType != 'all':
      acceptableStatusTypes.append('all')
      error( "'%s' is a wrong value for switch 'statusType'.\n\tThe acceptable values are:\n\t%s"
             % ( statusType, str( acceptableStatusTypes ) ) )


  if 'all' in statusType:
    return acceptableStatusTypes
  else:
    return statusTypes


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

def tabularPrint( table ):
  
  columns_names = table[0].keys()
  records = []
  for row in table:
    record = []
    for k,v in row.items():
      if type( v ) == datetime.datetime:
        record.append( Time.toString( v ) )
      elif v is None:
        record.append( '' )
      else:
        record.append( v )  
    records.append( record )    

  output = printTable( columns_names, records, numbering = False, 
                       columnSeparator = " | ", printOut = False )
  
  subLogger.notice( output )

#...............................................................................

def select( args, switchDict ):
  '''
    Given the switches, request a query 'select' on the ResourceStatusDB
    that gets from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()

  meta = { 'columns' : [ 'name', 'statusType', 'status', 'elementType', 'reason',
                         'dateEffective', 'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] }
   
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
                                          #tokenExpiration = switchDict[ 'tokenExpiration' ],
                                          meta = meta )
  result['output'] = [ dict( zip( output[ 'Columns' ], e ) ) for e in output[ 'Value' ] ]
  result['output'] = filterReason( result['output'], switchDict[ 'reason' ] )
  result['match'] = len( result['output'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

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
                                               #dateEffective = switchDict[ 'dateEffective' ],
                                               #lastCheckTime = switchDict[ 'lastCheckTime' ],
                                               tokenOwner = getToken( 'owner' ),
                                               tokenExpiration = getToken( 'expiration' )
                                              )

  if 'Value' in output:
    result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


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
                                          #dateEffective = switchDict[ 'dateEffective' ],
                                          #lastCheckTime = switchDict[ 'lastCheckTime' ],
                                          tokenOwner = getToken( 'owner' ),
                                          tokenExpiration = getToken( 'expiration' )
                                         )

  if 'Value' in output:
    result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


def delete( args, switchDict ):
  '''
    Given the switches, request a query 'delete' on the ResourceStatusDB
    that deletes from <element><tableType> all rows that match the parameters given.
  '''

  rssClient = ResourceStatusClient.ResourceStatusClient()


  result = { 'output': None, 'successful': None, 'message': None, 'match': None }
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

  if 'Value' in output:
    result['match'] = int( output['Value'] )
  result['successful'] = output['OK']
  result['message'] = output['Message'] if 'Message' in output else None

  return result


#...............................................................................

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
    tabularPrint( table )
  confirm( query, matches )

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
  run( args, switchDictSet )

  #Bye
  DIRACExit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
