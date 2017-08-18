''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

__RCSID__ = "$Id$"

from sqlalchemy.orm                                import sessionmaker, \
							  scoped_session
from sqlalchemy.sql                                import update, delete, select, and_, or_
from sqlalchemy.sql.expression import null
from sqlalchemy.inspection                         import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative                    import declarative_base
from sqlalchemy.dialects.mysql.base                import DOUBLE
from sqlalchemy                                    import create_engine, Table, Column, MetaData, String, \
                                                          DateTime, exc, Integer, Text

from DIRAC                                         import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities    import getDBParameters

# Defining the tables

metadata = MetaData()
rmsBase = declarative_base()

class AccountingCache(rmsBase):
  """ AccountingCache table
  """

  __tablename__ = 'AccountingCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
		    'mysql_charset': 'utf8'}

  name = Column( 'Name', String( 64 ), nullable = False, primary_key = True )
  plotName = Column( 'PlotName', String( 64 ), nullable = False, primary_key = True )
  plotType = Column( 'PlotType', String( 16 ), nullable = False, primary_key = True )
  lastCheckTime = Column( 'LastCheckTime', DateTime, nullable = False ) #FIXME: Need to add CURRENT_TIMESTAMP as default value
  result = Column( 'Result', Text, nullable = False )
  dateEffective = Column( 'DateEffective', DateTime, nullable = False )

  def __init__( self, result = null(), plotName = null(), plotType = null() ):
    self.result = result
    self.plotName = plotName
    self.plotType = plotType


  def fromDict( self, dictionary ):
    """
    Fill the fields of the Host object from a dictionary
    The dictionary may contain the keys: HostID, HostName, CPU
    """

    self.name = dictionary.get( 'Name', self.name )
    self.lastCheckTime = dictionary.get( 'LastCheckTime', self.lastCheckTime )
    self.plotName = dictionary.get( 'PlotName', self.plotName )
    self.result = dictionary.get( 'Result', self.result )
    self.dateEffective = dictionary.get( 'DateEffective', self.dateEffective )
    self.plotType = dictionary.get( 'PlotType', self.plotType )


    # # Metadata instance that is used to bind the engine, Object and tables
    # self.metadata = MetaData()
    #
    # DowntimeCache = Table( 'DowntimeCache', self.metadata,
    #                        Column( 'StartDate', DateTime, nullable = False ),
    #                        Column( 'DowntimeID', String( 64 ), nullable = False, primary_key = True ),
    #                        Column( 'Link', String( 255 ), nullable = False ),
    #                        Column( 'EndDate', DateTime, nullable = False ),
    #                        Column( 'Name', String( 64 ), nullable = False ),
    #                        Column( 'DateEffective', DateTime, nullable = False ),
    #                        Column( 'Description', String( 512 ), nullable = False ),
    #                        Column( 'Severity', String( 32 ), nullable = False ),
    #                        Column( 'LastCheckTime', DateTime, nullable = False ),
    #                        Column( 'Element', String( 32 ), nullable = False ),
    #                        Column( 'GOCDBServiceType', String( 32 ), nullable = False ),
    #                        mysql_engine = 'InnoDB' )
    #
    # GGUSTicketsCache = Table( 'GGUSTicketsCache', self.metadata,
    #                           Column( 'Tickets', String( 1024 ), nullable = False ),
    #                           Column( 'OpenTickets', Integer, nullable = False, server_default = '0'),
    #                           Column( 'GocSite', String( 64 ), nullable = False, primary_key = True ),
    #                           Column( 'Link', String( 1024 ), nullable = False ),
    #                           Column( 'LastCheckTime', DateTime, nullable = False ),
    #                           mysql_engine = 'InnoDB' )
    #
    # JobCache = Table( 'JobCache', self.metadata,
    #                   Column( 'Status', String( 16 ), nullable = False ),
    #                   Column( 'Efficiency', DOUBLE(asdecimal=False), nullable = False, server_default = '0'),
    #                   Column( 'MaskStatus', String( 32 ), nullable = False ),
    #                   Column( 'Site', String( 64 ), nullable = False, primary_key = True ),
    #                   Column( 'LastCheckTime', DateTime, nullable = False ),
    #                   mysql_engine = 'InnoDB' )
    #
    # PilotCache = Table('PilotCache', self.metadata,
    #                    Column( 'Status', String( 16 ), nullable = False ),
    #                    Column( 'LastCheckTime', DateTime, nullable = False ),
    #                    Column( 'Site', String( 64 ), nullable = False, primary_key = True ),
    #                    Column( 'CE', String( 64 ), nullable = False, primary_key = True ),
    #                    Column( 'PilotsPerJob', DOUBLE(asdecimal=False), nullable = False, server_default = '0'),
    #                    Column( 'PilotJobEff', DOUBLE(asdecimal=False), nullable = False, server_default = '0' ),
    #                    mysql_engine = 'InnoDB')
    #
    # PolicyResult = Table( 'PolicyResult', self.metadata,
    #                       Column( 'Status', String( 16 ), nullable = False ),
    #                       Column( 'PolicyName', String( 64 ), nullable = False, primary_key = True ),
    #                       Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' ),
    #                       Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
    #                       Column( 'DateEffective', DateTime, nullable = False ),
    #                       Column( 'StatusType', String( 16 ), nullable = False, server_default = '', primary_key = True ),
    #                       Column( 'LastCheckTime', DateTime, nullable = False ),
    #                       Column( 'Element', String( 32 ), nullable = False, primary_key = True ),
    #                       mysql_engine = 'InnoDB' )
    #
    # SpaceTokenOccupancyCache = Table( 'SpaceTokenOccupancyCache', self.metadata,
    #                                   Column( 'Endpoint', String( 128 ), nullable = False, primary_key = True ),
    #                                   Column( 'LastCheckTime', DateTime, nullable = False ),
    #                                   Column( 'Guaranteed', DOUBLE(asdecimal=False), nullable = False, server_default = '0' ),
    #                                   Column( 'Free', DOUBLE(asdecimal=False), nullable = False, server_default = '0' ),
    #                                   Column( 'Token', String( 64 ), nullable = False, primary_key = True ),
    #                                   Column( 'Total', DOUBLE(asdecimal=False), nullable = False, server_default = '0'),
    #                                   mysql_engine = 'InnoDB' )
    #
    # TransferCache = Table( 'TransferCache', self.metadata,
    #                        Column( 'SourceName', String( 64 ), nullable = False, primary_key = True ),
    #                        Column( 'LastCheckTime', DateTime, nullable = False ),
    #                        Column( 'Metric', String( 16 ), nullable = False, primary_key = True ),
    #                        Column( 'Value', DOUBLE(asdecimal=False), nullable = False, server_default = '0' ),
    #                        Column( 'DestinationName', String( 64 ), nullable = False, primary_key = True ),
    #                        mysql_engine = 'InnoDB' )
    #
    # UserRegistryCache = Table( 'UserRegistryCache', self.metadata,
    #                            Column( 'Login', String( 14 ), primary_key = True ),
    #                            Column( 'Name', String( 64 ), nullable = False ),
    #                            Column( 'LastCheckTime', DateTime, nullable = False ),
    #                            Column( 'Email', String( 64 ), nullable = False ),
    #                            mysql_engine = 'InnoDB' )
    #
    # ErrorReportBuffer = Table( 'ErrorReportBuffer', self.metadata,
    #                            Column( 'ErrorMessage', String( 512 ), nullable = False ),
    #                            Column( 'Name', String( 64 ), nullable = False ),
    #                            Column( 'DateEffective', DateTime, nullable = False ),
    #                            Column( 'Reporter', String( 64 ), nullable = False ),
    #                            Column( 'Operation', String( 64 ), nullable = False ),
    #                            Column( 'ElementType', String( 32 ), nullable = False ),
    #                            Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
    #                            Column( 'Arguments', String( 512 ), nullable = False, server_default = "" ),
    #                            mysql_engine = 'InnoDB' )
    #
    # PolicyResultWithID = Table('PolicyResultWithID', self.metadata,
    #                            Column( 'Status', String( 8 ), nullable = False ),
    #                            Column( 'PolicyName', String( 64 ), nullable = False ),
    #                            Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
    #                            Column( 'Name', String( 64 ), nullable = False ),
    #                            Column( 'DateEffective', DateTime, nullable = False ),
    #                            Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
    #                            Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
    #                            Column( 'LastCheckTime', DateTime, nullable = False ),
    #                            Column( 'Element', String( 32 ), nullable = False ),
    #                            mysql_engine = 'InnoDB' )
    #
    # PolicyResultLog = Table( 'PolicyResultLog', self.metadata,
    #                          Column( 'Status', String( 8 ), nullable = False ),
    #                          Column( 'PolicyName', String( 64 ), nullable = False ),
    #                          Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
    #                          Column( 'Name', String( 64 ), nullable = False ),
    #                          Column( 'DateEffective', DateTime, nullable = False ),
    #                          Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
    #                          Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
    #                          Column( 'LastCheckTime', DateTime, nullable = False ),
    #                          Column( 'Element', String( 32 ), nullable = False ),
    #                          mysql_engine = 'InnoDB' )
    #
    # PolicyResultHistory = Table( 'PolicyResultHistory', self.metadata,
    #                              Column( 'Status', String( 8 ), nullable = False ),
    #                              Column( 'PolicyName', String( 64 ), nullable = False ),
    #                              Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
    #                              Column( 'Name', String( 64 ), nullable = False ),
    #                              Column( 'DateEffective', DateTime, nullable = False ),
    #                              Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
    #                              Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
    #                              Column( 'LastCheckTime', DateTime, nullable = False ),
    #                              Column( 'Element', String( 32 ), nullable = False ),
    #                              mysql_engine = 'InnoDB' )




# Helper functions

def primaryKeystoList(table, **kwargs):
  '''

  Helper function that gets keyword arguments and adds to a
  list only the primary keys of a given table.

  :param table: <string>
  :param kwargs:
  :return: <list>
  '''

  primarykeys = []
  for primarykey in inspect(table).primary_key:
    primarykeys.append(primarykey.name)

  filters = []
  for name, argument in kwargs.items():
    if argument:
      if name in primarykeys:
        filters.append( getattr(table.c, name) == argument )

  return filters

def toList(table, **kwargs):
  '''
  Helper function that gets keyword arguments and adds them to a list
  that is going to be used to complete the sqlalchemy query.

  :param table: object of type <class 'sqlalchemy.sql.schema.Table'>
  :param kwargs: keyword arguments (DB columns)
  :return: <list> of sqlalchemy sqlalchemy.sql.elements.BinaryExpression objects
  '''

  filters = []
  for name, argument in kwargs.iteritems():
    if name == "Meta":

      if argument and 'older' in argument:
        # match everything that is older than the specified column name
        filters.append( getattr(table.c, argument['older'][0]) > argument['older'][1] )
        # argument['older'][0] must match a column name, otherwise this is going to fail
      elif argument and 'newer' in argument:
        # match everything that is newer than the specified column name
        filters.append( getattr(table.c, argument['newer'][0]) < argument['newer'][1] )
      else:
        continue

    else:
      if argument:
        filters.append( getattr(table.c, name) == argument )

  return filters

def toDict(**kwargs):
  '''
  Helper function that gets keyword arguments and adds them to a dictionary.

  :param table: <string>
  :param kwargs:
  :return: <list>
  '''

  params = {}
  for name, argument in kwargs.items():
    if argument:
      params.update( {name : argument} )

  return params

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
				 pool_recycle = 3600, echo_pool = True)
    self.session = scoped_session( sessionmaker( bind = self.engine ) )
    self.inspector = Inspector.from_engine( self.engine )


  def __initializeDB( self ):
    """
    Create the tables
    """

    tablesInDB = self.inspector.get_table_names()

    # Components
    if 'AccountingCache' not in tablesInDB:
      try:
	AccountingCache.__table__.create( self.engine ) #pylint: disable=no-member
      except Exception as e:
	return S_ERROR( e )
    else:
      gLogger.debug( 'Table \'AccountingCache\' already exists' )


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
    session = self.session( expire_on_commit = False )
    table = AccountingCache() #FIXME: I need to take it from __getattr__
    table.fromDict(params)

    try:
      session.add(table)
      session.commit()
      return S_OK()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "insert: unexpected exception", lException = e )
      return S_ERROR( "insert: unexpected exception %s" % e )
    finally:
      session.close()

  def selectPrimaryKeys( self, table, **kwargs ):
    '''
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQL buildCondition parser and generate a more sophisticated query.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    session = self.session()

    try:

      table = self.metadata.tables.get( table )

      args = primaryKeystoList(table, **kwargs)

      result = session.query( table ).filter(*args)

      arr = []

      for u in result:
        rel = []
        for j in u:
         rel.append(j)

        arr.append(rel)

      return S_OK( arr )

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )
    finally:
      session.close()

  def select( self, table, **kwargs ):
    '''
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQL buildCondition parser and generate a more sophisticated query.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
      **meta** - `dict`
        metadata for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.
    :return: S_OK() || S_ERROR()
    '''

    session = self.session()

    try:

      meta = False

      table = self.metadata.tables.get( table )

      args = toList(table, **kwargs)

      # this is the variable where we store the column names that correspond to the values that we are going to return
      columnNames = []

      columns = []
      for name, argument in kwargs.items():
        if argument and name == "Meta" and 'columns' in argument:
          meta = True
          for column in argument['columns']:
            columns.append( getattr(table.c, column) )
            columnNames.append( column )

      if meta:
        result = session.execute( select( columns )
                                  .where( and_(*args) ) )
      else :
        result = session.query( table ).filter(*args)

        for name in table.columns.keys():
          columnNames.append( str(name) )

      arr = []

      for u in result:
        rel = []
        for j in u:
          rel.append(j)

        arr.append(rel)

      finalResult = S_OK( arr )

      # add column names
      finalResult['Columns'] = columnNames

      return finalResult

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )
    finally:
      session.close()

  def update( self, table, **kwargs ):
    '''
    Updates row with values given on args. The row selection is done using the
    default of MySQLMonkey ( column.primary or column.keyColumn ). It can be
    modified using kwargs. The 'table' keyword argument is mandatory, and
    filled automatically by the Client. Typically you will not pass kwargs to
    this function, unless you know what are you doing and you have a very
    special use case.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.session( expire_on_commit = False )

    try:

      table = self.metadata.tables.get( table )

      args = primaryKeystoList(table, **kwargs)

      # fields to be updated
      params = toDict( **kwargs )

      session.execute( update( table )
                       .where( and_(*args) )
                       .values( **params ) )

      session.commit()
      session.expunge_all()

      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "update: unexpected exception", lException = e )
      return S_ERROR( "update: unexpected exception %s" % e )
    finally:
      session.close()

  def delete( self, table, **kwargs ):
    """
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQL buildCondition parser and generate a more sophisticated query.
    There is only one forbidden query, with all parameters None ( this would
    mean a query of the type DELETE * from TableName ). The usage of kwargs
    is the same as in the get function.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    """

    session = self.session()

    try:

      table = self.metadata.tables.get( table )

      args = toList(table, **kwargs)

      session.execute( delete( table )
                       .where( or_(*args) ) )

      session.commit()
      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "delete: unexpected exception", lException = e )
      return S_ERROR( "delete: unexpected exception %s" % e )
    finally:
      session.close()

  ## Extended SQL methods ######################################################

  def addOrModify( self, table, **kwargs ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is there, it is updated, if not, it is inserted as a new entry.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    try:

      result = self.selectPrimaryKeys( table, **kwargs )

      if not result['OK']:
        return result

      if not result['Value']:
        self.insert( table, **kwargs )
      else:
        self.update( table, **kwargs )

    except exc.SQLAlchemyError as e:
      self.log.exception( "addOrModify: unexpected exception", lException = e )
      return S_ERROR( "addOrModify: unexpected exception %s" % e )

    return S_OK()


  def addIfNotThere( self, table, **kwargs ):
    '''
    Using the PrimaryKeys of the table, it looks for the record in the database.
    If it is not there, it is inserted as a new entry.
    :Parameters:
      **params** - `dict`
        arguments for the mysql query ( must match table columns ! ).
    :return: S_OK() || S_ERROR()
    '''

    try:

      result = self.select( table, **kwargs )

      if not result['OK']:
        return result

      if not result['Value']:
        self.insert( table, **kwargs )

    except exc.SQLAlchemyError as e:
      self.log.exception( "addIfNotThere: unexpected exception", lException = e )
      return S_ERROR( "addIfNotThere: unexpected exception %s" % e )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
