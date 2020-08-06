''' ResourceStatusHandler

  Module that allows users to access the ResourceStatusDB remotely.

'''

# pylint: disable=too-many-arguments

__RCSID__ = '$Id$'

import six

from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

db = None


def convert(table, params):
  """ Conversion utility for backward compatibility
  """
  gLogger.debug("Calls from old client")
  # In this case, "params" contain the "meta", so we at least need to swap!
  tableFromOldCall = params['table']
  columnsFromOldCall = params.get('columns')
  olderFromOldCall = params.get('older')
  newerFromOldCall = params.get('newer')
  orderFromOldCall = params.get('order')
  limitFromOldCall = params.get('limit')
  params = table
  if columnsFromOldCall or olderFromOldCall or newerFromOldCall or orderFromOldCall or limitFromOldCall:
    params['Meta'] = {}
    if columnsFromOldCall:
      params['Meta']['columns'] = columnsFromOldCall
    if olderFromOldCall:
      params['Meta']['older'] = olderFromOldCall
    if newerFromOldCall:
      params['Meta']['newer'] = newerFromOldCall
    if orderFromOldCall:
      params['Meta']['order'] = orderFromOldCall
    if limitFromOldCall:
      params['Meta']['limit'] = limitFromOldCall
  table = tableFromOldCall

  return params, table


def loadResourceStatusComponent(moduleName, className):
  """
  Create an object of a given database component.

  :param moduleName: module name to be loaded
  :param className: class name
  :return: object instance wrapped in a standard Dirac return object.
  """

  objectLoader = ObjectLoader()
  componentModule = 'ResourceStatusSystem.DB.%s' % (moduleName,)
  result = objectLoader.loadObject(componentModule, className)
  if not result['OK']:
    gLogger.error('Failed to load RSS component', '%s: %s' % (moduleName, result['Message']))
    return result
  componentClass = result['Value']
  component = componentClass()
  return S_OK(component)


def initializeResourceStatusHandler(serviceInfo):
  """
    Handler initialization, where we:
      dynamically load ResourceStatus database plugin module, as advised by the config,
      (assumes that the module name and a class name are the same)
      set the ResourceManagementDB as global db.

      :param serviceInfo: service info dictionary
      :return: standard Dirac return object

  """

  gLogger.debug("Initializing ResourceStatus Service with the following DB component:")
  defaultOption, defaultClass = 'ResourceStatusDB', 'ResourceStatusDB'
  configValue = getServiceOption(serviceInfo, defaultOption, defaultClass)
  gLogger.debug("Option:%-20s Class: %-20s" % (str(defaultOption), str(configValue)))
  result = loadResourceStatusComponent(configValue, configValue)

  if not result['OK']:
    return result

  global db
  db = result['Value']

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

  types_insert = [[six.string_types, dict], dict]

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

    if isinstance(table, dict):  # for backward compatibility: conversion is needed
      params, table = convert(table, params)

    gLogger.info('insert: %s %s' % (table, params))
    res = db.insert(table, params)
    self.__logResult('insert', res)

    return res

  types_select = [[six.string_types, dict], dict]

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

    if isinstance(table, dict):  # for backward compatibility: conversion is needed
      params, table = convert(table, params)

    gLogger.info('select: %s %s' % (table, params))
    res = db.select(table, params)
    self.__logResult('select', res)

    return res

  types_delete = [[six.string_types, dict], dict]

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

    if isinstance(table, dict):  # for backward compatibility: conversion is needed
      params, table = convert(table, params)

    gLogger.info('delete: %s %s' % (table, params))
    res = db.delete(table, params)
    self.__logResult('delete', res)

    return res

  types_addOrModify = [[six.string_types, dict], dict]

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

    if isinstance(table, dict):  # for backward compatibility: conversion is needed
      params, table = convert(table, params)

    gLogger.info('addOrModify: %s %s' % (table, params))
    res = db.addOrModify(table, params)
    self.__logResult('addOrModify', res)

    return res

  types_addIfNotThere = [[six.string_types, dict], dict]

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

    if isinstance(table, dict):  # for backward compatibility: conversion is needed
      params, table = convert(table, params)

    gLogger.info('addIfNotThere: %s %s' % (table, params))
    res = db.addIfNotThere(table, params)
    self.__logResult('addIfNotThere', res)

    return res
