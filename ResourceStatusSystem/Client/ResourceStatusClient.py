# $HeadURL:  $
''' ResourceStatusClient

  Client to interact with the ResourceStatusDB.

'''

from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                             import RPCClient

from DIRAC.ResourceStatusSystem.Utilities                   import RssConfiguration
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.FrameworkSystem.Client.NotificationClient        import NotificationClient


__RCSID__ = '$Id:  $'

class ResourceStatusClient( object ):
  """
  The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus`
  API. All functions you need are on this client.

  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - select
   - delete

  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.

  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceStatusDB` and :class:`ResourceStatusHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
   >>> rsClient = ResourceStatusClient()

  All functions calling methods exposed on the database or on the booster are
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.
  """

  def __init__( self , serviceIn = None ):
    '''
      The client tries to connect to :class:ResourceStatusDB by default. If it
      fails, then tries to connect to the Service :class:ResourceStatusHandler.
    '''

    self.rssDB = RPCClient( "ResourceStatus/ResourceStatus" )

    self.validElements = RssConfiguration.getValidElements()

  ################################################################################
  # Element status methods - enjoy !

  def insertStatusElement( self, element, tableType, name, statusType, status,
                           elementType, reason, dateEffective, lastCheckTime,
                           tokenOwner, tokenExpiration ):
    '''
    Inserts on <element><tableType> a new row with the arguments given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.insert(element, tableType, name, statusType, status,
                             elementType, reason, dateEffective, lastCheckTime,
                             tokenOwner, tokenExpiration)

  def updateStatusElement( self, element, tableType, name = None, statusType = None,
                           status = None, elementType = None, reason = None,
                           dateEffective = None, lastCheckTime = None,
                           tokenOwner = None, tokenExpiration = None ):
    '''
    Updates <element><tableType> with the parameters given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.update(element, tableType, name, statusType, status,
                             elementType, reason, dateEffective, lastCheckTime,
                             tokenOwner, tokenExpiration)

  def selectStatusElement( self, element, tableType, name = None, statusType = None,
                           status = None, elementType = None, reason = None,
                           dateEffective = None, lastCheckTime = None,
                           tokenOwner = None, tokenExpiration = None ):
    '''
    Gets from <element><tableType> all rows that match the parameters given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `[, string, list]`
        name of the individual of class element
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.select(element, tableType, name, statusType, status,
                             elementType, reason, dateEffective, lastCheckTime,
                             tokenOwner, tokenExpiration)

  def deleteStatusElement( self, element, tableType, name = None, statusType = None,
                           status = None, elementType = None, reason = None,
                           dateEffective = None, lastCheckTime = None,
                           tokenOwner = None, tokenExpiration = None ):
    '''
    Deletes from <element><tableType> all rows that match the parameters given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `[, string, list]`
        name of the individual of class element
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.delete(element, tableType, name, statusType, status,
                             elementType, reason, dateEffective, lastCheckTime,
                             tokenOwner, tokenExpiration)

  def addOrModifyStatusElement( self, element, tableType, name = None,
                                statusType = None, status = None,
                                elementType = None, reason = None,
                                dateEffective = None, lastCheckTime = None,
                                tokenOwner = None, tokenExpiration = None ):
    '''
    Adds or updates-if-duplicated from <element><tableType> and also adds a log
    if flag is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.addOrModify(element, tableType, name, statusType, status,
                                  elementType, reason, dateEffective, lastCheckTime,
                                  tokenOwner, tokenExpiration)

  def modifyStatusElement( self, element, tableType, name = None, statusType = None,
                           status = None, elementType = None, reason = None,
                           dateEffective = None, lastCheckTime = None, tokenOwner = None,
                           tokenExpiration = None ):
    '''
    Updates from <element><tableType> and also adds a log if flag is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.modify(element, tableType, name, statusType, status,
                             elementType, reason, dateEffective, lastCheckTime,
                             tokenOwner, tokenExpiration)

  def addIfNotThereStatusElement( self, element, tableType, name = None,
                                  statusType = None, status = None,
                                  elementType = None, reason = None,
                                  dateEffective = None, lastCheckTime = None,
                                  tokenOwner = None, tokenExpiration = None ):
    '''
    Adds if-not-duplicated from <element><tableType> and also adds a log if flag
    is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable=unused-argument
    return self.rssDB.addIfNotThere(element, tableType, name, statusType, status,
                                    elementType, reason, dateEffective, lastCheckTime,
                                    tokenOwner, tokenExpiration)




  ##############################################################################
  # Protected methods - Use carefully !!

  def notify( self, request, params ):
    ''' Send notification for a given request with its params to the diracAdmin
    '''
    address = Operations().getValue( 'ResourceStatus/Notification/DebugGroup/Users' )
    msg = 'Matching parameters: ' + str( params )
    sbj = '[NOTIFICATION] DIRAC ResourceStatusDB: ' + request + ' entry'
    NotificationClient().sendMail( address, sbj, msg , address )

  def _extermineStatusElement( self, element, name, keepLogs = True ):
    '''
    Deletes from <element>Status,
                 <element>History
                 <element>Log
     all rows with `elementName`. It removes all the entries, logs, etc..
    Use with common sense !

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElements ), any of the defaults: \
          `Site` | `Resource` | `Node`
      **name** - `[, string, list]`
        name of the individual of class element
      **keepLogs** - `bool`
        if active, logs are kept in the database

    :return: S_OK() || S_ERROR()
    '''
    return self.__extermineStatusElement( element, name, keepLogs )

  def _query( self, queryType, parameters ):
    '''
    It is a simple helper, this way inheriting classes can use it.
    '''
    return self.__query( queryType, parameters )

  ##############################################################################
  # Private methods - where magic happens ;)

  def __query( self, queryType, parameters ):
    '''
      This method is a rather important one. It will format the input for the DB
      queries, instead of doing it on a decorator. Two dictionaries must be passed
      to the DB. First one contains 'columnName' : value pairs, being the key
      lower camel case. The second one must have, at lease, a key named 'table'
      with the right table name.
    '''
    # Functions we can call, just a light safety measure.
    _gateFunctions = [ 'insert', 'update', 'select', 'delete', 'addOrModify', 'modify', 'addIfNotThere' ]
    if not queryType in _gateFunctions:
      return S_ERROR( '"%s" is not a proper gate call' % queryType )

    gateFunction = getattr( self.gate, queryType )

    # If meta is None, we set it to {}
    meta = ( True and parameters.pop( 'meta' ) ) or {}
    # Remove self, added by locals()
    del parameters[ 'self' ]

    # This is an special case with the Element tables.
    #if tableName.startswith( 'Element' ):
    element   = parameters.pop( 'element' )
    if not element in self.validElements:
      gLogger.debug( '"%s" is not a valid element like %s' % ( element, self.validElements ) )
      return S_ERROR( '"%s" is not a valid element like %s' % ( element, self.validElements ) )

    tableType = parameters.pop( 'tableType' )
    #tableName = tableName.replace( 'Element', element )
    tableName = '%s%s' % ( element, tableType )

    meta[ 'table' ] = tableName

    gLogger.debug( 'Calling %s, with \n params %s \n meta %s' % ( queryType, parameters, meta ) )
    userRes = gateFunction( parameters, meta )

    return userRes

  def __extermineStatusElement( self, element, name, keepLogs ):
    '''
      This method iterates over the three ( or four ) table types - depending
      on the value of keepLogs - deleting all matches of `name`.
    '''

    tableTypes = [ 'Status', 'History' ]
    if keepLogs is False:
      tableTypes.append( 'Log' )

    for table in tableTypes:

      deleteQuery = self.deleteStatusElement( element, table, name = name )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
