""" ResourceManagementDB:
    This module provides definition of the DB tables, and methods to access them.

    Written using sqlalchemy declarative_base



    For extending the ResourceStatusDB tables:

    1) In the extended module, call:

    from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import rmsBase, TABLESLIST
    TABLESLIST = TABLESLIST + [list of new table names]

    2) provide a declarative_base definition of the tables (new or extended) in the extension module

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import datetime
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, exc, Text, Integer, Float

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.SQLAlchemyDB import SQLAlchemyDB
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

  downtimeid = Column('DowntimeID', String(127), nullable=False, primary_key=True)
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

  columnsOrder = ['Site', 'CE', 'Status', 'PilotJobEff', 'PilotsPerJob', 'LastCheckTime', 'VO']

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
    return [self.site, self.ce, self.status, self.pilotjobeff, self.pilotsperjob, self.lastchecktime, self.vo]


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
            self.status, self.reason, self.dateeffective, self.lastchecktime, self.vo]


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


class ResourceManagementDB(SQLAlchemyDB):
  """
    Class that defines the methods to interact to the ResourceManagementDB tables
  """

  def __init__(self):
    """c'tor

    :param self: self reference
    """

    super(ResourceManagementDB, self).__init__()

    # This is the list of tables that will be created.
    # It can be extended in an extension module
    self.tablesList = getattr(Utils.voimport('DIRAC.ResourceStatusSystem.DB.ResourceManagementDB'),
                              'TABLESLIST')
    self._initializeConnection('ResourceStatus/ResourceManagementDB')

    # Create required tables
    self._createTablesIfNotThere(self.tablesList)
  # Extended SQL methods ######################################################

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
      for columnName, columnValue in params.items():
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
      for columnName, columnValue in params.items():
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
