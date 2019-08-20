''' ResourceStatusHandler

  Module that allows users to access the ResourceStatusDB remotely.

'''

# pylint: disable=too-many-arguments

__RCSID__ = '$Id$'

from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB


db = None


def initializeResourceStatusHandler(_serviceInfo):
  '''
    Handler initialization, where we set the ResourceStatusDB as global db, and
    we instantiate the synchronizer.
  '''

  global db
  db = ResourceStatusDB()

  return S_OK()

################################################################################


class ResourceStatusHandler(RequestHandler):
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

  def __init__(self, *args, **kwargs):

    super(ResourceStatusHandler, self).__init__(*args, **kwargs)

  @staticmethod
  def __logResult(methodName, result):
    '''
      Method that writes to log error messages
    '''

    if not result['OK']:
      gLogger.error('%s%s' % (methodName, result['Message']))

  @staticmethod
  def setDatabase(database):
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

  types_insert = [[basestring, dict], dict]

  def export_insert(self, table, params):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It
    does not add neither processing nor validation. If you need to know more
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info('insert: %s %s' % (table, params))
    res = db.insert(table, params)
    self.__logResult('insert', res)

    return res

  types_select = [[basestring, dict], dict]

  def export_select(self, table, params):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely. It
    does not add neither processing nor validation. If you need to know more
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

    :return: S_OK() || S_ERROR()
    '''

    gLogger.info('select: %s %s' % (table, params))
    res = db.select(table, params)
    self.__logResult('select', res)

    return res

  types_delete = [[basestring, dict], dict]

  def export_delete(self, table, params):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    '''

    gLogger.info('delete: %s %s' % (table, params))
    res = db.delete(table, params)
    self.__logResult('delete', res)

    return res

  types_addOrModify = [[basestring, dict], dict]

  def export_addOrModify(self, table, params):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    '''

    gLogger.info('addOrModify: %s %s' % (table, params))
    res = db.addOrModify(table, params)
    self.__logResult('addOrModify', res)

    return res

  types_addIfNotThere = [[basestring, dict], dict]

  def export_addIfNotThere(self, table, params):
    '''
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    '''

    gLogger.info('addIfNotThere: %s %s' % (table, params))
    res = db.addIfNotThere(table, params)
    self.__logResult('addIfNotThere', res)

    return res
