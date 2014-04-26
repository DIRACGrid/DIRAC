# $HeadURL:  $
''' ResourceStatusClient

  Client to interact with the ResourceStatusDB.

'''

from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                             import RPCClient
#from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB  import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities                   import RssConfiguration
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.FrameworkSystem.Client.NotificationClient        import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites

from types import StringTypes, ListType

__RCSID__ = '$Id:  $'
       
class ResourceStatusClient( object ):
  '''
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
  '''

  def __init__( self , serviceIn = None ):
    '''
      The client tries to connect to :class:ResourceStatusDB by default. If it 
      fails, then tries to connect to the Service :class:ResourceStatusHandler.
    '''
    
    if not serviceIn:
      #self.gate = ResourceStatusDB()           
      self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
    else:
      self.gate = serviceIn 

    self.validElements = RssConfiguration.getValidElements()

  ################################################################################
  # Element status methods - enjoy ! 
  
  def insertStatusElement( self, element, tableType, name, statusType, status, 
                           elementType, reason, dateEffective, lastCheckTime, 
                           tokenOwner, tokenExpiration, meta = None ): 
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'insert', locals() )
  def updateStatusElement( self, element, tableType, name, statusType, status, 
                           elementType, reason, dateEffective, lastCheckTime, 
                           tokenOwner, tokenExpiration, meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'update', locals() )
  def selectStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, 
                           tokenOwner = None, tokenExpiration = None, meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', locals() )
  def deleteStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, 
                           tokenOwner = None, tokenExpiration = None, meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613

    result = self._query( 'delete', locals() )
    if result['OK']:
      self.notify( 'delete', str( locals() ) )
    return result

  def addOrModifyStatusElement( self, element, tableType, name = None, 
                                statusType = None, status = None, 
                                elementType = None, reason = None, 
                                dateEffective = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', locals() )

  def modifyStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'modify', locals() )  
  def addIfNotThereStatusElement( self, element, tableType, name = None, 
                                  statusType = None, status = None, 
                                  elementType = None, reason = None, 
                                  dateEffective = None, lastCheckTime = None, 
                                  tokenOwner = None, tokenExpiration = None, 
                                  meta = None ):
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
        column to distinguish between the diferent elements in the same element
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
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addIfNotThere', locals() )

  ##############################################################################
  # Protected methods - Use carefully !!

  def notify( self, request, params ):
    '''
      Send notification for a given request with its params to the diracAdmin
    '''

    mail = NotificationClient()
    address = Operations().getValue( 'ResourceStatus/Notification/DebugGroup/Users' )
    msg = 'Matching parameters: ' + str( params )
    sbj = '[NOTIFICATION] DIRAC ResourceStatusDB: ' + request + ' entry'
    mail.sendMail( address, sbj, msg , address )

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
    
    # For Site elements always use the short names
    if element == "Site" and parameters['name'] is not None:
      if type( parameters['name'] ) in StringTypes:
        parameters['name'] = [parameters['name']]
      if type( parameters['name'] ) == ListType:
        result = getSites( parameters['name'] )
        if not result['OK']:
          gLogger.debug( result['Message'] )            
          return result
        parameters['name'] = result['Value']  
      else:
        gLogger.debug( 'Invalid site name type: %s' % type( parameters['name'] ) )            
        return S_ERROR( 'Invalid site name type: %s' % type( parameters['name'] ) )    
    
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
    if keepLogs == False:
      tableTypes.append( 'Log' )
    
    for table in tableTypes:
      
      deleteQuery = self.deleteStatusElement( element, table, name = name )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery

    return S_OK()
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    
