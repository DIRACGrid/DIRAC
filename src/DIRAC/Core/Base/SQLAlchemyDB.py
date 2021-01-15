""" SQLAlchemyDB:
    This module provides the BaseRSSDB class for providing standard DB interactions.

    Uses sqlalchemy
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import datetime
from sqlalchemy import create_engine, desc, exc
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DIRACDB import DIRACDB
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters


class SQLAlchemyDB(DIRACDB):
  """
    Base class that defines some of the basic DB interactions.
  """

  def __init__(self, *args, **kwargs):
    """c'tor

    :param self: self reference
    """
    self.fullname = self.__class__.__name__
    super(SQLAlchemyDB, self).__init__(*args, **kwargs)

    self.extensions = gConfig.getValue('DIRAC/Extensions', [])
    self.tablesList = []

  def _initializeConnection(self, dbPath):
    """
    Collect from the CS all the info needed to connect to the DB.
    """

    result = getDBParameters(dbPath)
    if not result['OK']:
      raise Exception('Cannot get database parameters: %s' % result['Message'])

    dbParameters = result['Value']
    self.log.debug("db parameters: %s" % dbParameters)
    self.host = dbParameters['Host']
    self.port = dbParameters['Port']
    self.user = dbParameters['User']
    self.password = dbParameters['Password']
    self.dbName = dbParameters['DBName']

    self.engine = create_engine('mysql://%s:%s@%s:%s/%s' % (self.user,
                                                            self.password,
                                                            self.host,
                                                            self.port,
                                                            self.dbName),
                                pool_recycle=3600,
                                echo_pool=True,
                                echo=self.log.getLevel() == 'DEBUG')
    self.sessionMaker_o = sessionmaker(bind=self.engine)
    self.inspector = Inspector.from_engine(self.engine)

  def _createTablesIfNotThere(self, tablesList):
    """
    Adds each table in tablesList to the DB if not already present
    """
    tablesInDB = self.inspector.get_table_names()

    for table in tablesList:
      if table not in tablesInDB:
        found = False
        # is it in the extension? (fully or extended)
        for ext in self.extensions:
          try:
            getattr(
                __import__(
                    ext + self.__class__.__module__,
                    globals(),
                    locals(),
                    [table]),
                table).__table__.create(
                    self.engine)  # pylint: disable=no-member
            found = True
            break
          except (ImportError, AttributeError):
            continue
        # If not found in extensions, import it from DIRAC base.
        if not found:
          getattr(
              __import__(
                  self.__class__.__module__,
                  globals(),
                  locals(),
                  [table]),
              table).__table__.create(
                  self.engine)  # pylint: disable=no-member
      else:
        gLogger.debug("Table %s already exists" % table)

  def insert(self, table, params):
    """
    Inserts params in the DB.

    :param str table: table where to insert
    :param dict params: Dictionary to fill a single line

    :return: S_OK() || S_ERROR()
    """

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.sessionMaker_o(expire_on_commit=False)  # FIXME: should we use this flag elsewhere?

    found = False
    for ext in self.extensions:
      try:
        tableRow_o = getattr(
            __import__(ext + self.__class__.__module__, globals(), locals(), [table]),
            table)()
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      tableRow_o = getattr(
          __import__(self.__class__.__module__, globals(), locals(), [table]),
          table)()

    tableRow_o.fromDict(params)

    try:
      session.add(tableRow_o)
      session.commit()
      return S_OK()
    except exc.IntegrityError as err:
      self.log.warn("insert: trying to insert a duplicate key? %s" % err)
      session.rollback()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("insert: unexpected exception", lException=e)
      return S_ERROR("insert: unexpected exception %s" % e)
    finally:
      session.close()

  def select(self, table, params):
    """
    Uses params to build conditional SQL statement ( WHERE ... ).

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

    :return: S_OK() || S_ERROR()
    """

    session = self.sessionMaker_o()

    # finding the table
    found = False
    for ext in self.extensions:
      try:
        table_c = getattr(
            __import__(ext + self.__class__.__module__, globals(), locals(), [table]),
            table)
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      table_c = getattr(
          __import__(self.__class__.__module__, globals(), locals(), [table]),
          table)

    # handling query conditions found in 'Meta'
    columnNames = [column.lower() for column in params.get('Meta', {}).get('columns', [])]
    older = params.get('Meta', {}).get('older', None)
    newer = params.get('Meta', {}).get('newer', None)
    order = params.get('Meta', {}).get('order', None)
    limit = params.get('Meta', {}).get('limit', None)
    params.pop('Meta', None)

    try:
      # setting up the select query
      if not columnNames:  # query on the whole table
        wholeTable = True
        try:
          columnNames = table_c.columnsOrder  # see ResourceStatusDB for example
        except AttributeError:
          columns = table_c.__table__.columns  # retrieve the column names
          columnNames = [str(column).split('.')[1] for column in columns]
        select = Query(table_c, session=session)
      else:  # query only the selected columns
        wholeTable = False
        columns = [getattr(table_c, column) for column in columnNames]
        select = Query(columns, session=session)

      # query conditions
      for columnName, columnValue in params.items():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, six.string_types + (datetime.datetime, bool)):
          select = select.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" % type(columnValue))
      if older:
        column_a = getattr(table_c, older[0].lower())
        select = select.filter(column_a < older[1])
      if newer:
        column_a = getattr(table_c, newer[0].lower())
        select = select.filter(column_a > newer[1])
      if order:
        order = [order] if isinstance(order, six.string_types) else list(order)
        column_a = getattr(table_c, order[0].lower())
        if len(order) == 2 and order[1].lower() == 'desc':
          select = select.order_by(desc(column_a))
        else:
          select = select.order_by(column_a)
      if limit:
        select = select.limit(int(limit))

      # querying
      selectionRes = select.all()

      # handling the results
      if wholeTable:
        selectionResToList = [res.toList() for res in selectionRes]
      else:
        selectionResToList = [[getattr(res, col) for col in columnNames] for res in selectionRes]

      finalResult = S_OK(selectionResToList)

      finalResult['Columns'] = columnNames
      return finalResult

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("select: unexpected exception", lException=e)
      return S_ERROR("select: unexpected exception %s" % e)
    finally:
      session.close()

  def delete(self, table, params):
    """
    :param table: table from where to delete
    :type table: str
    :param params: dictionary of which line(s) to delete
    :type params: dict

    :return: S_OK() || S_ERROR()
    """
    session = self.sessionMaker_o()

    found = False
    for ext in self.extensions:
      try:
        table_c = getattr(
            __import__(ext + self.__class__.__module__, globals(), locals(), [table]),
            table)
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      table_c = getattr(
          __import__(self.__class__.__module__, globals(), locals(), [table]),
          table)

    # handling query conditions found in 'Meta'
    older = params.get('Meta', {}).get('older', None)
    newer = params.get('Meta', {}).get('newer', None)
    order = params.get('Meta', {}).get('order', None)
    limit = params.get('Meta', {}).get('limit', None)
    params.pop('Meta', None)

    try:
      deleteQuery = Query(table_c, session=session)
      for columnName, columnValue in params.items():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          deleteQuery = deleteQuery.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, six.string_types + (datetime.datetime, bool)):
          deleteQuery = deleteQuery.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" % type(columnValue))
      if older:
        column_a = getattr(table_c, older[0].lower())
        deleteQuery = deleteQuery.filter(column_a < older[1])
      if newer:
        column_a = getattr(table_c, newer[0].lower())
        deleteQuery = deleteQuery.filter(column_a > newer[1])
      if order:
        order = [order] if isinstance(order, six.string_types) else list(order)
        column_a = getattr(table_c, order[0].lower())
        if len(order) == 2 and order[1].lower() == 'desc':
          deleteQuery = deleteQuery.order_by(desc(column_a))
        else:
          deleteQuery = deleteQuery.order_by(column_a)
      if limit:
        deleteQuery = deleteQuery.limit(int(limit))

      res = deleteQuery.delete(synchronize_session=False)  # FIXME: unsure about it
      session.commit()
      return S_OK(res)

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("delete: unexpected exception", lException=e)
      return S_ERROR("delete: unexpected exception %s" % e)
    finally:
      session.close()
