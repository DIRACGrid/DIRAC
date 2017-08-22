''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

__RCSID__ = "$Id$"

import datetime
from sqlalchemy.orm                                import sessionmaker, \
                                                          scoped_session, \
                                                          query
from sqlalchemy.sql.expression import null
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative                    import declarative_base
from sqlalchemy                                    import create_engine, Column, MetaData, String, \
                                                          DateTime, exc, Text, and_, or_, inspect

from DIRAC                                         import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities    import getDBParameters

# Defining the tables
#FIXME: need to add all the tables

metadata = MetaData()
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
    The dictionary may contain the keys: Name, LastCheckTime, PlotName, Result, DateEffective, PlotType
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

  def __init__( self, name = null() ):
    self.name = name

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
                                 pool_recycle = 3600, echo_pool = True, echo = True)
    self.sessionMaker_o = sessionmaker( bind = self.engine )
    self.inspector = Inspector.from_engine( self.engine )


  def __initializeDB( self ):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    for table in ['AccountingCache', 'DowntimeCache']:
      if table not in tablesInDB:
        getattr(__import__(__name__, globals(), locals(), [table]), table).__table__.create( self.engine ) #pylint: disable=no-member
      else:
        gLogger.debug( 'Table \'%s\' already exists' %table )


 # SQL Methods ###############################################################

  def insert( self, table, params ):
    '''
    Inserts args in the DB making use of kwargs where parameters such as
    the 'table' are specified ( filled automatically by the Client). Typically you
    will not pass kwargs to this function, unless you know what are you doing
    and you have a very special use case.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.sessionMaker_o( expire_on_commit = False )
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
    #FIXME: this stuff about META and columns ... probably for the web?

    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    try:
      select = session.query(table_c)
      for columnName, columnValue in params.iteritems():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, basestring):
          select = select.filter(column_a == columnValue)
        elif isinstance(columnValue, datetime.datetime): #FIXME: iis it correct/enough? (should check also below)
          select = select.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" %type(columnValue))

      listOfRows = [res.toList() for res in select.all()]

      finalResult = S_OK( listOfRows )

      # add column names
      finalResult['Columns'] = ['columnNames'] #FIXME: put real stuff

      return finalResult


    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )
    finally:
      session.close()

  def delete( self, table, params ):
    """
    Uses arguments to build conditional SQL statement ( WHERE ... ).

    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
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
          select = deleteQuery.filter(column_a == columnValue)
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
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    try:
      select = session.query(table_c) #FIXME: Should be done only for primary keys
      for columnName, columnValue in params.iteritems():
        if not columnValue:
          continue
        column_a = getattr(table_c, columnName.lower())
        if isinstance(columnValue, (list, tuple)):
          select = select.filter(column_a.in_(list(columnValue)))
        elif isinstance(columnValue, basestring):
          select = select.filter(column_a == columnValue)
        else:
          self.log.error("type(columnValue) == %s" %type(columnValue))

      res = select.first()
      if not res:
        return self.insert(table, params)

      for columnName, columnValue in params.iteritems():
        column_a = getattr(table_c, columnName.lower())
        column_a = columnValue

      session.commit()
      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "addOrModify: unexpected exception", lException = e )
      return S_ERROR( "addOrModify: unexpected exception %s" % e )
    finally:
      session.close()

  def addIfNotThere( self, table, params ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is not there, it is inserted as a new entry.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''
    session = self.sessionMaker_o()
    table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)

    try:
      session.commit()
      return S_OK()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "addIfNotThere: unexpected exception", lException = e )
      return S_ERROR( "addIfNotThere: unexpected exception %s" % e )
    finally:
      session.close()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
