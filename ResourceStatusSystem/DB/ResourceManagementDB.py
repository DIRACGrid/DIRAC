''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

__RCSID__ = "$Id$"

import datetime
from sqlalchemy.orm import sessionmaker, class_mapper
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, DateTime, exc, Text, Integer, Float

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

# Defining the tables

rmsBase = declarative_base()

class AccountingCache(rmsBase):
  """ AccountingCache table
  """

  __tablename__ = 'AccountingCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column( 'Name', String( 64 ), nullable = False, primary_key = True )
  plotname = Column( 'PlotName', String( 64 ), nullable = False, primary_key = True )
  plottype = Column( 'PlotType', String( 16 ), nullable = False, primary_key = True )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False ) #FIXME: Need to add CURRENT_TIMESTAMP as default value
  result = Column( 'Result', Text, nullable = False )
  dateeffective = Column( 'DateEffective', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the AccountingCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.name = dictionary.get( 'Name', self.name )
    self.plotname = dictionary.get( 'PlotName', self.plotname )
    self.plottype = dictionary.get( 'PlotType', self.plottype )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )
    self.result = dictionary.get( 'Result', self.result )
    self.dateeffective = dictionary.get( 'DateEffective', self.dateeffective )

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

  downtimeid = Column( 'DowntimeID', String( 64 ), nullable = False, primary_key = True )
  name = Column( 'Name', String( 64 ), nullable = False )
  element = Column( 'Element', String( 32 ), nullable = False )
  gocdbservicetype = Column( 'GOCDBServiceType', String( 32 ), nullable = False )
  severity = Column( 'Severity', String( 32 ), nullable = False )
  description = Column( 'Description', String( 512 ), nullable =False )
  link = Column( 'Link', String( 255 ), nullable = False )
  startdate = Column( 'StartDate', DateTime, nullable = False )
  enddate = Column( 'EndDate', DateTime, nullable = False )
  dateeffective = Column( 'DateEffective', DateTime, nullable = False )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the DowntimeCache object from a dictionary
    """

    self.downtimeid = dictionary.get( 'DowntimeID', self.downtimeid )
    self.name = dictionary.get( 'Name', self.name )
    self.element = dictionary.get( 'Element', self.element )
    self.gocdbservicetype = dictionary.get( 'GOCDBServiceType', self.gocdbservicetype )
    self.severity = dictionary.get( 'Severity', self.severity )
    self.description = dictionary.get( 'Description', self.description )
    self.link = dictionary.get( 'Link', self.link )
    self.startdate = dictionary.get( 'StartDate', self.startdate )
    self.enddate = dictionary.get( 'EndDate', self.enddate )
    self.dateeffective = dictionary.get( 'DateEffective', self.dateeffective )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

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

  gocsite = Column( 'GocSite', String( 64 ), nullable = False, primary_key = True )
  tickets = Column( 'Tickets', String( 1024 ), nullable = False )
  opentickets = Column( 'OpenTickets', Integer, nullable = False, server_default = '0')
  link = Column( 'Link', String( 1024 ), nullable = False )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the GGUSTicketsCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.tickets = dictionary.get( 'Tickets', self.tickets )
    self.opentickets = dictionary.get( 'OpenTickets', self.opentickets )
    self.gocsite = dictionary.get( 'GocSite', self.gocsite )
    self.link = dictionary.get( 'Link', self.link )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

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

  site = Column( 'Site', String( 64 ), nullable = False, primary_key = True )
  status = Column( 'Status', String( 16 ), nullable = False )
  efficiency = Column( 'Efficiency', Float(asdecimal=False), nullable = False, server_default = '0')
  maskstatus = Column( 'MaskStatus', String( 32 ), nullable = False )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the JobCache object from a dictionary
    """

    self.site = dictionary.get( 'Site', self.site )
    self.status = dictionary.get( 'Status', self.status )
    self.efficiency = dictionary.get( 'Efficiency', self.efficiency )
    self.maskstatus = dictionary.get( 'MaskStatus', self.maskstatus )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

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

  site = Column( 'Site', String( 64 ), nullable = False, primary_key = True )
  ce = Column( 'CE', String( 64 ), nullable = False, primary_key = True )
  status = Column( 'Status', String( 16 ), nullable = False )
  pilotjobeff = Column( 'PilotJobEff', Float(asdecimal=False), nullable = False, server_default = '0' )
  pilotsperjob = Column( 'PilotsPerJob', Float(asdecimal=False), nullable = False, server_default = '0')
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the PilotCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.site = dictionary.get( 'Site', self.site )
    self.ce = dictionary.get( 'CE', self.ce )
    self.status = dictionary.get( 'Status', self.status )
    self.pilotjobeff = dictionary.get( 'PilotJobEff', self.pilotjobeff )
    self.pilotsperjob = dictionary.get( 'PilotsPerJob', self.pilotsperjob )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.site, self.ce, self.status, self.pilotjobeff, self.pilotsperjob, self.lastchecktime]


class PolicyResult(rmsBase):
  """ PolicyResult table
  """

  __tablename__ = 'PolicyResult'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  policyname = Column( 'PolicyName', String( 64 ), nullable = False, primary_key = True )
  statustype = Column( 'StatusType', String( 16 ), nullable = False, server_default = '', primary_key = True )
  element = Column( 'Element', String( 32 ), nullable = False, primary_key = True )
  name = Column( 'Name', String( 64 ), nullable = False, primary_key = True )
  status = Column( 'Status', String( 16 ), nullable = False )
  reason = Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' )
  dateeffective = Column( 'DateEffective', DateTime, nullable = False )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the PolicyResult object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.policyname = dictionary.get( 'PolicyName', self.policyname )
    self.statustype = dictionary.get( 'StatusType', self.statustype )
    self.element = dictionary.get( 'Element', self.element )
    self.name = dictionary.get( 'Name', self.name )
    self.status = dictionary.get( 'Status', self.status )
    self.reason = dictionary.get( 'Reason', self.reason )
    self.dateeffective = dictionary.get( 'DateEffective', self.dateeffective )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.policyname, self.statustype, self.element, self.name,
            self.status, self.reason, self.dateeffective, self.lastchecktime]


class SpaceTokenOccupancyCache(rmsBase):
  """ SpaceTokenOccupancyCache table
  """

  __tablename__ = 'SpaceTokenOccupancyCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  endpoint = Column( 'Endpoint', String( 128 ), nullable = False, primary_key = True )
  token = Column( 'Token', String( 64 ), nullable = False, primary_key = True )
  guaranteed = Column( 'Guaranteed', Float(asdecimal=False), nullable = False, server_default = '0' )
  free = Column( 'Free', Float(asdecimal=False), nullable = False, server_default = '0' )
  total = Column( 'Total', Float(asdecimal=False), nullable = False, server_default = '0')
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the SpaceTokenOccupancyCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.endpoint = dictionary.get( 'Endpoint', self.endpoint )
    self.token = dictionary.get( 'Token', self.token )
    self.guaranteed = dictionary.get( 'Guaranteed', self.guaranteed )
    self.free = dictionary.get( 'Free', self.free )
    self.total = dictionary.get( 'Total', self.total )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

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

  sourcename = Column( 'SourceName', String( 64 ), nullable = False, primary_key = True )
  destinationname = Column( 'DestinationName', String( 64 ), nullable = False, primary_key = True )
  metric = Column( 'Metric', String( 16 ), nullable = False, primary_key = True )
  value = Column( 'Value', Float(asdecimal=False), nullable = False, server_default = '0' )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the TransferCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.sourcename = dictionary.get( 'SourceName', self.sourcename )
    self.destinationname = dictionary.get( 'DestinationName', self.destinationname )
    self.metric = dictionary.get( 'Metric', self.metric )
    self.value = dictionary.get( 'Value', self.value )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.sourcename, self.destinationname, self.metric, self.value, self.lastchecktime]



class ResourceManagementDB( object ):
  '''
    Class that defines the tables for the ResourceManagementDB on a python dictionary.
  '''

  def __init__( self ):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'ResourceManagementDB' )

    self.__initializeConnection( 'ResourceStatus/ResourceManagementDB' )
    self.__initializeDB()

  def __initializeConnection( self, dbPath ):
    """ Collect from the CS all the info needed to connect to the DB.
    This should be in a base class eventually
    """

    result = getDBParameters( dbPath )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.log.debug("db parameters: %s" % dbParameters)
    self.host = dbParameters[ 'Host' ]
    self.port = dbParameters[ 'Port' ]
    self.user = dbParameters[ 'User' ]
    self.password = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]

    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.user,
                                                              self.password,
                                                              self.host,
                                                              self.port,
                                                              self.dbName ),
                                 pool_recycle = 3600,
                                 echo_pool = True,
                                 echo = self.log.getLevel() == 'DEBUG')
    self.sessionMaker_o = sessionmaker( bind = self.engine )
    self.inspector = Inspector.from_engine( self.engine )


  def __initializeDB( self ):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    for table in ['AccountingCache',
                  'DowntimeCache',
                  'GGUSTicketsCache',
                  'JobCache',
                  'PilotCache',
                  'PolicyResult',
                  'SpaceTokenOccupancyCache',
                  'TransferCache']:
      if table not in tablesInDB:
        getattr(__import__(__name__, globals(), locals(), [table]), table).__table__.create( self.engine ) #pylint: disable=no-member
      else:
        gLogger.debug( 'Table \'%s\' already exists' %table )


 # SQL Methods ###############################################################

  def insert( self, table, params ):
    '''
    Inserts params in the DB.

    :param table: table where to insert
    :type table: str
    :param params: Dictionary to fill a single line
    :type params: dict

    :return: S_OK() || S_ERROR()
    '''

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.sessionMaker_o( expire_on_commit = False ) #FIXME: should we use this flag elsewhere?
    tableRow_o = getattr(__import__(__name__, globals(), locals(), [table]), table)()
    tableRow_o.fromDict(params)

    try:
      session.add(tableRow_o)
      session.commit()
      return S_OK()
    except exc.IntegrityError as err:
      self.log.warn("insert: trying to insert a duplicate key? %s" %err)
      session.rollback()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "insert: unexpected exception", lException = e )
      return S_ERROR( "insert: unexpected exception %s" % e )
    finally:
      session.close()

  def select( self, table, params ):
    '''
    Uses params to build conditional SQL statement ( WHERE ... ).

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).

    :return: S_OK() || S_ERROR()
    '''

    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    columnNames = []

    try:
      select = session.query(table_c)
      for columnName, columnValue in params.iteritems():
        if columnName.lower() == 'meta' and columnValue: # special case
          columnNames = columnValue['columns']
        else: # these are real columns
          if not columnValue:
            continue
          column_a = getattr(table_c, columnName.lower())
          if isinstance(columnValue, (list, tuple)):
            select = select.filter(column_a.in_(list(columnValue)))
          elif isinstance(columnValue, basestring):
            select = select.filter(column_a == columnValue)
          elif isinstance(columnValue, datetime.datetime): #FIXME: is it correct/enough? (should check also below)
            select = select.filter(column_a == columnValue)
          else:
            self.log.error("type(columnValue) == %s" %type(columnValue))

      listOfRows = [res.toList() for res in select.all()]
      finalResult = S_OK( listOfRows )

      if not columnNames:
        # retrieve the column names
        columns = table_c.__table__.columns
        columnNames = [str(column) for column in columns]

      finalResult['Columns'] = columnNames
      return finalResult

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )
    finally:
      session.close()

  def delete( self, table, params ):
    """
    :param table: table from where to delete
    :type table: str
    :param params: dictionary of which line(s) to delete
    :type params: dict

    :return: S_OK() || S_ERROR()
    """
    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    try:
      deleteQuery = session.query(table_c)
      for columnName, columnValue in params.iteritems():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          deleteQuery = deleteQuery.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, basestring):
          deleteQuery = deleteQuery.filter(column_a == columnValue)
        elif isinstance(columnValue, datetime.datetime):
          deleteQuery = deleteQuery.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" %type(columnValue))

      res = deleteQuery.delete(synchronize_session=False) #FIXME: unsure about it
      session.commit()
      return S_OK(res)


    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "delete: unexpected exception", lException = e )
      return S_ERROR( "delete: unexpected exception %s" % e )
    finally:
      session.close()

  ## Extended SQL methods ######################################################

  def addOrModify( self, table, params ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is there, it is updated, if not, it is inserted as a new entry.

    :param table: table where to add or modify
    :type table: str
    :param params: dictionary of what to add or modify
    :type params: dict

    :return: S_OK() || S_ERROR()
    '''

    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)
    primaryKeys = [key.name for key in class_mapper(table_c).primary_key]

    try:
      select = session.query(table_c)
      for columnName, columnValue in params.iteritems():
        if not columnValue or columnName not in primaryKeys:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, basestring):
          select = select.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" %type(columnValue))

      res = select.first() # the selection is done via primaryKeys only
      if not res: # if not there, let's insert it
        return self.insert(table, params)

      # now we assume we need to modify
      for columnName, columnValue in params.iteritems():
        if columnValue:
          setattr(res, columnName.lower(), columnValue)

      session.commit()
      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "addOrModify: unexpected exception", lException = e )
      return S_ERROR( "addOrModify: unexpected exception %s" % e )
    finally:
      session.close()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
