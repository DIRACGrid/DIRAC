''' ResourceStatusHandler

  Module that allows users to access the ResourceStatusDB remotely.

'''

#pylint: disable=too-many-arguments

__RCSID__ = '$Id: $'

import datetime

from DIRAC                                             import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB    import ResourceStatusDB


db = None

def convert(params, meta):
  """ Conversion utility for backward compatibility
  """
  element, tableType, name, statusType, status, \
  elementType, reason, dateEffective, lastCheckTime, \
  tokenOwner, tokenExpiration = [params.get(k) for k in ['element', 'tableType', 'name', 'statusType', 'status', \
                                                         'elementType', 'reason', 'dateEffective', 'lastCheckTime', \
                                                         'tokenOwner', 'tokenExpiration']]

  element = 'Resource' # in v6r17 and below, element is always 'Resource'
  tableType = meta['table'].replace('Resource', '')

  return element, tableType, name, statusType, status, \
  elementType, reason, dateEffective, lastCheckTime, \
  tokenOwner, tokenExpiration, meta

def initializeResourceStatusHandler( _serviceInfo ):
  '''
    Handler initialization, where we set the ResourceStatusDB as global db, and
    we instantiate the synchronizer.
  '''

  global db
  db = ResourceStatusDB()

  return S_OK()

################################################################################

class ResourceStatusHandler( RequestHandler ):
  '''
  The ResourceStatusHandler exposes the DB front-end functions through a XML-RPC
  server, functionalities inherited from
  :class:`DIRAC.Core.DISET.RequestHandler.RequestHandler`

  According to the ResourceStatusDB philosophy, only functions of the type:
  - insert
  - update
  - select
  - delete

  are exposed. If you need anything more complicated, either look for it on the
  :class:`ResourceStatusClient`, or code it yourself. This way the DB and the
  Service are kept clean and tidied.

  To can use this service on this way, but you MUST NOT DO IT. Use it through the
  :class:`ResourceStatusClient`. If offers in the worst case as good performance
  as the :class:`ResourceStatusHandler`, if not better.

   >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
   >>> server = RPCCLient( "ResourceStatus/ResourceStatus" )
  '''

  def __init__( self, *args, **kwargs ):

    # create tables for empty db
    db.createTables()

    super( ResourceStatusHandler, self ).__init__( *args, **kwargs )

  @staticmethod
  def __logResult( methodName, result ):
    '''
      Method that writes to log error messages
    '''

    if not result[ 'OK' ]:
      gLogger.error( '%s%s' % ( methodName, result[ 'Message' ] ) )

  @staticmethod
  def setDatabase( database ):
    '''
    This method let us inherit from this class and overwrite the database object
    without having problems with the global variables.

    :Parameters:
      **database** - `MySQL`
        database used by this handler

    :return: None
    '''
    global db
    db = database

  types_insert = [ basestring, basestring, basestring, basestring, basestring, basestring,
                   basestring, datetime.datetime, datetime.datetime, basestring, datetime.datetime]
  def export_insert( self, element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)


    gLogger.info( 'insert: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.insert( element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration )

    self.__logResult( 'insert', res )

    return res

  types_update = [ basestring, basestring ]
  def export_update( self, element, tableType, name = None, statusType = None,
                     status = None, elementType = None, reason = None,
                     dateEffective = None, lastCheckTime = None,
                     tokenOwner = None, tokenExpiration = None, ID = None ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)


    gLogger.info( 'update: %s %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration, ID ) )

    res = db.update( element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration, ID )

    self.__logResult( 'update', res )

    return res

  types_select = [ [basestring, dict], [basestring, dict, None] ]
  def export_select( self, element, tableType, name = None, statusType = None,
                     status = None, elementType = None, reason = None,
                     dateEffective = None, lastCheckTime = None,
                     tokenOwner = None, tokenExpiration = None, meta = None ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

    :Parameters:
      **element** - `string` or `dict`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` | `Resource` | `Node`
        if it is a dict, then it's a dictionary of parameters
        (here for backward compatibility with versions prior to DIRAC v6r18)
      **tableType** - `string` or `dict`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
        if it is a dict, then it's a dictionary of meta (see last parameter of this same method)
        (here for backward compatibility with versions prior to DIRAC v6r18)
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
      **meta** - `dict`
        metadata for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

    :return: S_OK() || S_ERROR()
    '''

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)


    gLogger.info( 'select: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.select( element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration, meta )

    self.__logResult( 'select', res )

    return res

  types_delete = [ basestring, basestring ]

  def export_delete( self, element, tableType, name = None, statusType = None,
                     status = None, elementType = None, reason = None,
                     dateEffective = None, lastCheckTime = None,
                     tokenOwner = None, tokenExpiration = None, meta = None):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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
      **meta** - `dict`
        metadata for the mysql query

    :return: S_OK() || S_ERROR()
    '''

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)

    gLogger.info( 'delete: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.delete( element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration, meta )

    self.__logResult( 'delete', res )

    return res

  types_addOrModify = [ basestring, basestring ]

  def export_addOrModify( self, element, tableType, name = None, statusType = None,
                          status = None, elementType = None, reason = None,
                          dateEffective = None, lastCheckTime = None,
                          tokenOwner = None, tokenExpiration = None ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)

    gLogger.info( 'addOrModify: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.addOrModify( element, tableType, name, statusType, status,
                          elementType, reason, dateEffective, lastCheckTime,
                          tokenOwner, tokenExpiration )

    self.__logResult( 'addOrModify', res )

    return res

  types_modify = [ basestring, basestring ]

  def export_modify( self, element, tableType, name = None, statusType = None,
                     status = None, elementType = None, reason = None,
                     dateEffective = None, lastCheckTime = None,
                     tokenOwner = None, tokenExpiration = None ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)

    gLogger.info( 'modify: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.modify( element, tableType, name, statusType, status,
                     elementType, reason, dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration )

    self.__logResult( 'modify', res )

    return res

  types_addIfNotThere = [ basestring, basestring ]

  def export_addIfNotThere( self, element, tableType, name = None, statusType = None,
                            status = None, elementType = None, reason = None,
                            dateEffective = None, lastCheckTime = None,
                            tokenOwner = None, tokenExpiration = None ):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It does
    not add neither processing nor validation. If you need to know more about
    this method, you must keep reading on the database documentation.

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

    if isinstance(element, dict): #for backward compatibility - converting to str variables
      gLogger.debug("Calls from old client")
      #element is the old "params" in this case
      element, tableType, name, statusType, status, \
      elementType, reason, dateEffective, lastCheckTime, \
      tokenOwner, tokenExpiration, meta = convert(element, tableType)

    gLogger.info( 'addIfNotThere: %s %s %s %s %s %s %s %s %s %s %s' %
                  ( element, tableType, name, statusType, status,
                    elementType, reason, dateEffective, lastCheckTime,
                    tokenOwner, tokenExpiration ) )

    res = db.addIfNotThere( element, tableType, name, statusType, status,
                            elementType, reason, dateEffective, lastCheckTime,
                            tokenOwner, tokenExpiration )

    self.__logResult( 'addIfNotThere', res )

    return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
