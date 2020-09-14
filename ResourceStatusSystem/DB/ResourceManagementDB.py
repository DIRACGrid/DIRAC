""" ResourceManagementDB:
    This module provides definition of the DB tables, and methods to access them.

    Written using sqlalchemy declarative_base



    For extending the ResourceStatusDB tables:

    1) In the extended module, call:

    from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import rmsBase, TABLESLIST
    TABLESLIST = TABLESLIST + [list of new table names]

    2) provide a declarative_base definition of the tables (new or extended) in the extension module

"""

__RCSID__ = "$Id$"

import six
import datetime
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker, class_mapper
from sqlalchemy.orm.query import Query
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, DateTime, exc, Text, Integer, Float

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.ResourceStatusSystem.Utilities import Utils


# Defining the tables

TABLESLIST = ['AccountingCache',
              'DowntimeCache',
              'GGUSTicketsCache',
              'JobCache',
              'PilotCache',
              'PolicyResult',
              'SpaceTokenOccupancyCache',
              'TransferCache']

rmsBase = declarative_base()


class AccountingCache(rmsBase):
  """ AccountingCache table
  """

  __tablename__ = 'AccountingCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column('Name', String(64), nullable=False, primary_key=True)
  plotname = Column('PlotName', String(64), nullable=False, primary_key=True)
  plottype = Column('PlotType', String(16), nullable=False, primary_key=True)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)
  result = Column('Result', Text, nullable=False)
  dateeffective = Column('DateEffective', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the AccountingCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.name = dictionary.get('Name', self.name)
    self.plotname = dictionary.get('PlotName', self.plotname)
    self.plottype = dictionary.get('PlotType', self.plottype)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))
    self.result = dictionary.get('Result', self.result)
    self.dateeffective = dictionary.get('DateEffective', self.dateeffective.replace(microsecond=0)
                                        if self.dateeffective
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.name, self.plotname, self.plottype, self.lastchecktime, self.result, self.dateeffective]


class DowntimeCache(rmsBase):
  """ DowntimeCache table
  """

  __tablename__ = 'DowntimeCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  downtimeid = Column('DowntimeID', String(64), nullable=False, primary_key=True)
  name = Column('Name', String(64), nullable=False)
  element = Column('Element', String(32), nullable=False)
  gocdbservicetype = Column('GOCDBServiceType', String(32), nullable=True)
  severity = Column('Severity', String(32), nullable=False)
  description = Column('Description', String(512), nullable=False)
  link = Column('Link', String(255), nullable=True)
  startdate = Column('StartDate', DateTime, nullable=False)
  enddate = Column('EndDate', DateTime, nullable=False)
  dateeffective = Column('DateEffective', DateTime, nullable=False)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the DowntimeCache object from a dictionary
    """

    self.downtimeid = dictionary.get('DowntimeID', self.downtimeid)
    self.name = dictionary.get('Name', self.name)
    self.element = dictionary.get('Element', self.element)
    self.gocdbservicetype = dictionary.get('GOCDBServiceType', self.gocdbservicetype)
    self.severity = dictionary.get('Severity', self.severity)
    self.description = dictionary.get('Description', self.description)
    self.link = dictionary.get('Link', self.link)
    self.startdate = dictionary.get('StartDate', self.startdate)
    self.enddate = dictionary.get('EndDate', self.enddate)
    self.dateeffective = dictionary.get('DateEffective', self.dateeffective.replace(microsecond=0)
                                        if self.dateeffective
                                        else datetime.datetime.utcnow().replace(microsecond=0))
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.downtimeid, self.name, self.element, self.gocdbservicetype,
            self.severity, self.description, self.link,
            self.startdate, self.enddate, self.dateeffective, self.lastchecktime]


class GGUSTicketsCache(rmsBase):
  """ GGUSTicketsCache table
  """

  __tablename__ = 'GGUSTicketsCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  gocsite = Column('GocSite', String(64), nullable=False, primary_key=True)
  tickets = Column('Tickets', String(1024), nullable=False)
  opentickets = Column('OpenTickets', Integer, nullable=False, server_default='0')
  link = Column('Link', String(1024), nullable=False)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the GGUSTicketsCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.tickets = dictionary.get('Tickets', self.tickets)
    self.opentickets = dictionary.get('OpenTickets', self.opentickets)
    self.gocsite = dictionary.get('GocSite', self.gocsite)
    self.link = dictionary.get('Link', self.link)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.gocsite, self.tickets, self.opentickets, self.link, self.lastchecktime]


class JobCache(rmsBase):
  """ JobCache table
  """

  __tablename__ = 'JobCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  site = Column('Site', String(64), nullable=False, primary_key=True)
  status = Column('Status', String(16), nullable=False)
  efficiency = Column('Efficiency', Float(asdecimal=False), nullable=False, server_default='0')
  maskstatus = Column('MaskStatus', String(32), nullable=False)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the JobCache object from a dictionary
    """

    self.site = dictionary.get('Site', self.site)
    self.status = dictionary.get('Status', self.status)
    self.efficiency = dictionary.get('Efficiency', self.efficiency)
    self.maskstatus = dictionary.get('MaskStatus', self.maskstatus)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.site, self.status, self.efficiency, self.maskstatus, self.lastchecktime]


class PilotCache(rmsBase):
  """ PilotCache table
  """

  __tablename__ = 'PilotCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  site = Column('Site', String(64), nullable=False, primary_key=True)
  ce = Column('CE', String(64), nullable=False, primary_key=True)
  vo = Column('VO', String(64), nullable=False, primary_key=True, server_default='all')
  status = Column('Status', String(16), nullable=False)
  pilotjobeff = Column('PilotJobEff', Float(asdecimal=False), nullable=False, server_default='0')
  pilotsperjob = Column('PilotsPerJob', Float(asdecimal=False), nullable=False, server_default='0')
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the PilotCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.site = dictionary.get('Site', self.site)
    self.ce = dictionary.get('CE', self.ce)
    self.vo = dictionary.get('VO', self.vo)
    self.status = dictionary.get('Status', self.status)
    self.pilotjobeff = dictionary.get('PilotJobEff', self.pilotjobeff)
    self.pilotsperjob = dictionary.get('PilotsPerJob', self.pilotsperjob)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.site, self.ce, self.vo, self.status, self.pilotjobeff, self.pilotsperjob, self.lastchecktime]


class PolicyResult(rmsBase):
  """ PolicyResult table
  """

  __tablename__ = 'PolicyResult'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  policyname = Column('PolicyName', String(64), nullable=False, primary_key=True)
  statustype = Column('StatusType', String(16), nullable=False, server_default='', primary_key=True)
  element = Column('Element', String(32), nullable=False, primary_key=True)
  name = Column('Name', String(64), nullable=False, primary_key=True)
  vo = Column('VO', String(64), nullable=False, primary_key=True, server_default='all')
  status = Column('Status', String(16), nullable=False)
  reason = Column('Reason', String(512), nullable=False, server_default='Unspecified')
  dateeffective = Column('DateEffective', DateTime, nullable=False)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the PolicyResult object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.policyname = dictionary.get('PolicyName', self.policyname)
    self.statustype = dictionary.get('StatusType', self.statustype)
    self.element = dictionary.get('Element', self.element)
    self.name = dictionary.get('Name', self.name)
    self.vo = dictionary.get('VO', self.vo)
    self.status = dictionary.get('Status', self.status)
    self.reason = dictionary.get('Reason', self.reason)
    self.dateeffective = dictionary.get('DateEffective', self.dateeffective.replace(microsecond=0)
                                        if self.dateeffective
                                        else datetime.datetime.utcnow().replace(microsecond=0))
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.policyname, self.statustype, self.element, self.name,
            self.vo, self.status, self.reason, self.dateeffective, self.lastchecktime]


class SpaceTokenOccupancyCache(rmsBase):
  """ SpaceTokenOccupancyCache table
  """

  __tablename__ = 'SpaceTokenOccupancyCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  endpoint = Column('Endpoint', String(128), nullable=False, primary_key=True)
  token = Column('Token', String(64), nullable=False, primary_key=True)
  guaranteed = Column('Guaranteed', Float(asdecimal=False), nullable=False, server_default='0')
  free = Column('Free', Float(asdecimal=False), nullable=False, server_default='0')
  total = Column('Total', Float(asdecimal=False), nullable=False, server_default='0')
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the SpaceTokenOccupancyCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.endpoint = dictionary.get('Endpoint', self.endpoint)
    self.token = dictionary.get('Token', self.token)
    self.guaranteed = dictionary.get('Guaranteed', self.guaranteed)
    self.free = dictionary.get('Free', self.free)
    self.total = dictionary.get('Total', self.total)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.endpoint, self.token, self.guaranteed, self.free, self.total, self.lastchecktime]


class TransferCache(rmsBase):
  """ TransferCache table
  """

  __tablename__ = 'TransferCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  sourcename = Column('SourceName', String(64), nullable=False, primary_key=True)
  destinationname = Column('DestinationName', String(64), nullable=False, primary_key=True)
  metric = Column('Metric', String(16), nullable=False, primary_key=True)
  value = Column('Value', Float(asdecimal=False), nullable=False, server_default='0')
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the TransferCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.sourcename = dictionary.get('SourceName', self.sourcename)
    self.destinationname = dictionary.get('DestinationName', self.destinationname)
    self.metric = dictionary.get('Metric', self.metric)
    self.value = dictionary.get('Value', self.value)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime.replace(microsecond=0)
                                        if self.lastchecktime
                                        else datetime.datetime.utcnow().replace(microsecond=0))

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.sourcename, self.destinationname, self.metric, self.value, self.lastchecktime]


class ResourceManagementDB(object):
  """
    Class that defines the methods to interact to the ResourceManagementDB tables
  """

  def __init__(self):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger('ResourceManagementDB')

    # This is the list of tables that will be created.
    # It can be extended in an extension module
    self.tablesList = getattr(Utils.voimport('DIRAC.ResourceStatusSystem.DB.ResourceManagementDB'),
                              'TABLESLIST')

    self.extensions = gConfig.getValue('DIRAC/Extensions', [])
    self.__initializeConnection('ResourceStatus/ResourceManagementDB')
    self.__initializeDB()

  def __initializeConnection(self, dbPath):
    """
    Collects from the CS all the info needed to connect to the DB.
    This should be in a base class eventually
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

  def __initializeDB(self):
    """
    Creates the tables
    """

    tablesInDB = self.inspector.get_table_names()

    for table in self.tablesList:
      if table not in tablesInDB:
        found = False
        # is it in the extension? (fully or extended)
        for ext in self.extensions:
          try:
            getattr(
                __import__(
                    ext + __name__,
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
                  __name__,
                  globals(),
                  locals(),
                  [table]),
              table).__table__.create(
              self.engine)  # pylint: disable=no-member
      else:
        gLogger.debug("Table %s already exists" % table)

 # SQL Methods ###############################################################

  def insert(self, table, params):
    """
    Inserts params in the DB.

    :param table: table where to insert
    :type table: str
    :param params: Dictionary to fill a single line
    :type params: dict

    :return: S_OK() || S_ERROR()
    """

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.sessionMaker_o(expire_on_commit=False)  # FIXME: should we use this flag elsewhere?

    found = False
    for ext in self.extensions:
      try:
        tableRow_o = getattr(__import__(ext + __name__, globals(), locals(), [table]), table)()
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      tableRow_o = getattr(__import__(__name__, globals(), locals(), [table]), table)()

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
        table_c = getattr(__import__(ext + __name__, globals(), locals(), [table]), table)
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

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
        columns = table_c.__table__.columns  # retrieve the column names
        columnNames = [str(column).split('.')[1] for column in columns]
        select = Query(table_c, session=session)
      else:  # query only the selected columns
        wholeTable = False
        columns = [getattr(table_c, column) for column in columnNames]
        select = Query(columns, session=session)

      # query conditions
      for columnName, columnValue in params.iteritems():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, (basestring, datetime.datetime, bool)):
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
        table_c = getattr(__import__(ext + __name__, globals(), locals(), [table]), table)
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    # handling query conditions found in 'Meta'
    older = params.get('Meta', {}).get('older', None)
    newer = params.get('Meta', {}).get('newer', None)
    order = params.get('Meta', {}).get('order', None)
    limit = params.get('Meta', {}).get('limit', None)
    params.pop('Meta', None)

    try:
      deleteQuery = Query(table_c, session=session)
      for columnName, columnValue in params.iteritems():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          deleteQuery = deleteQuery.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, (basestring, datetime.datetime, bool)):
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

  ## Extended SQL methods ######################################################

  def addOrModify(self, table, params):
    """
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is there, it is updated, if not, it is inserted as a new entry.

    :param table: table where to add or modify
    :type table: str
    :param params: dictionary of what to add or modify
    :type params: dict

    :return: S_OK() || S_ERROR()
    """

    session = self.sessionMaker_o()

    found = False
    for ext in self.extensions:
      try:
        table_c = getattr(__import__(ext + __name__, globals(), locals(), [table]), table)
        found = True
        break
      except (ImportError, AttributeError):
        continue
    # If not found in extensions, import it from DIRAC base (this same module).
    if not found:
      table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    columns = [key.name for key in class_mapper(table_c).columns]
    primaryKeys = [key.name for key in class_mapper(table_c).primary_key]

    try:
      select = Query(table_c, session=session)
      for columnName, columnValue in params.iteritems():
        if not columnValue or columnName not in primaryKeys:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, six.string_types):
          select = select.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" % type(columnValue))

      res = select.first()  # the selection is done via primaryKeys only
      if not res:  # if not there, let's insert it
        return self.insert(table, params)

      # Treating case of time value updates
      if 'LastCheckTime' in columns and not params.get('LastCheckTime'):
        params['LastCheckTime'] = None
      if 'DateEffective' in columns and not params.get('DateEffective'):
        params['DateEffective'] = None

      # now we assume we need to modify
      for columnName, columnValue in params.iteritems():
        if columnName == 'LastCheckTime' and not columnValue:  # we always update lastCheckTime
          columnValue = datetime.datetime.utcnow().replace(microsecond=0)
        if columnName == 'DateEffective' and not columnValue:  # we always update DateEffective, if there
          columnValue = datetime.datetime.utcnow().replace(microsecond=0)
        if columnValue:
          setattr(res, columnName.lower(), columnValue)

      session.commit()
      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("addOrModify: unexpected exception", lException=e)
      return S_ERROR("addOrModify: unexpected exception %s" % e)
    finally:
      session.close()

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
