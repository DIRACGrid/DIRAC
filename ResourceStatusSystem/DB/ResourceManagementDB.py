''' ResourceManagementDB

  Module that provides basic methods to access the ResourceManagementDB.

'''

from datetime                                      import datetime

from DIRAC                                         import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities    import getDBParameters
from sqlalchemy.orm                                import sessionmaker
from sqlalchemy.sql                                import update, delete, select, and_, or_
from sqlalchemy.dialects.mysql                     import DOUBLE
from sqlalchemy.inspection                         import inspect
from sqlalchemy                                    import create_engine, Table, Column, MetaData, String, \
                                                          DateTime, exc, Integer, Text

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

  :param table: <string>
  :param kwargs:
  :return: <list>
  '''

  filters = []
  for name, argument in kwargs.items():
    if name == "Meta":
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

  def __getDBConnectionInfo( self, fullname ):
    """ Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
    """

    result = getDBParameters( fullname )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result[ 'Message' ] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]

  def __init__( self ):
    """c'tor

    :param self: self reference
    """

    # Metadata instance that is used to bind the engine, Object and tables
    self.metadata = MetaData()

    AccountingCache         = Table( 'AccountingCache', self.metadata,
                              Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'LastCheckTime', DateTime, nullable = False ),
                              Column( 'PlotName', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'Result', Text, nullable = False ),
                              Column( 'DateEffective', DateTime, nullable = False ),
                              Column( 'PlotType', String( 16 ), nullable = False, primary_key = True ),
                              mysql_engine = 'InnoDB' )

    DowntimeCache            = Table( 'DowntimeCache', self.metadata,
                               Column( 'StartDate', DateTime, nullable = False ),
                               Column( 'DowntimeID', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'Link', String( 255 ), nullable = False ),
                               Column( 'EndDate', DateTime, nullable = False ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'Description', String( 512 ), nullable = False ),
                               Column( 'Severity', String( 32 ), nullable = False ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Element', String( 32 ), nullable = False ),
                               Column( 'GOCDBServiceType', String( 32 ), nullable = False ),
                               mysql_engine = 'InnoDB' )

    GGUSTicketsCache         = Table( 'GGUSTicketsCache', self.metadata,
                               Column( 'Tickets', String( 1024 ), nullable = False ),
                               Column( 'OpenTickets', Integer, nullable = False, server_default = '0'),
                               Column( 'GocSite', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'Link', String( 1024 ), nullable = False ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               mysql_engine = 'InnoDB' )

    JobCache                 = Table( 'JobCache', self.metadata,
                               Column( 'Status', String( 16 ), nullable = False ),
                               Column( 'Efficiency', DOUBLE, nullable = False, server_default = '0'),
                               Column( 'MaskStatus', String( 32 ), nullable = False ),
                               Column( 'Site', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               mysql_engine = 'InnoDB' )

    PilotCache               = Table( 'PilotCache', self.metadata,
                               Column( 'Status', String( 16 ), nullable = False ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Site', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'CE', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'PilotsPerJob', DOUBLE, nullable = False, server_default = '0'),
                               Column( 'PilotJobEff', DOUBLE, nullable = False, server_default = '0' ),
                               mysql_engine = 'InnoDB' )

    PolicyResult             = Table( 'PolicyResult', self.metadata,
                               Column( 'Status', String( 16 ), nullable = False ),
                               Column( 'PolicyName', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' ),
                               Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'StatusType', String( 16 ), nullable = False, server_default = '', primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Element', String( 32 ), nullable = False, primary_key = True ),
                               mysql_engine = 'InnoDB' )

    SpaceTokenOccupancyCache = Table( 'SpaceTokenOccupancyCache', self.metadata,
                               Column( 'Endpoint', String( 128 ), nullable = False, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Guaranteed', DOUBLE, nullable = False, server_default = '0' ),
                               Column( 'Free', DOUBLE, nullable = False, server_default = '0' ),
                               Column( 'Token', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'Total', DOUBLE, nullable = False, server_default = '0'),
                               mysql_engine = 'InnoDB' )

    TransferCache            = Table( 'TransferCache', self.metadata,
                               Column( 'SourceName', String( 64 ), nullable = False, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Metric', String( 16 ), nullable = False, primary_key = True ),
                               Column( 'Value', DOUBLE, nullable = False, server_default = '0' ),
                               Column( 'DestinationName', String( 64 ), nullable = False, primary_key = True ),
                               mysql_engine = 'InnoDB' )

    UserRegistryCache        = Table( 'UserRegistryCache', self.metadata,
                               Column( 'Login', String( 14 ), primary_key = True ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Email', String( 64 ), nullable = False ),
                               mysql_engine = 'InnoDB' )

    ErrorReportBuffer        = Table( 'ErrorReportBuffer', self.metadata,
                               Column( 'ErrorMessage', String( 512 ), nullable = False ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'Reporter', String( 64 ), nullable = False ),
                               Column( 'Operation', String( 64 ), nullable = False ),
                               Column( 'ElementType', String( 32 ), nullable = False ),
                               Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
                               Column( 'Arguments', String( 512 ), nullable = False, server_default = "" ),
                               mysql_engine = 'InnoDB' )

    PolicyResultWithID        = Table( 'PolicyResultWithID', self.metadata,
                               Column( 'Status', String( 8 ), nullable = False ),
                               Column( 'PolicyName', String( 64 ), nullable = False ),
                               Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
                               Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Element', String( 32 ), nullable = False ),
                               mysql_engine = 'InnoDB' )

    PolicyResultLog          = Table( 'PolicyResultLog', self.metadata,
                               Column( 'Status', String( 8 ), nullable = False ),
                               Column( 'PolicyName', String( 64 ), nullable = False ),
                               Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
                               Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Element', String( 32 ), nullable = False ),
                               mysql_engine = 'InnoDB' )

    PolicyResultHistory      = Table( 'PolicyResultHistory', self.metadata,
                               Column( 'Status', String( 8 ), nullable = False ),
                               Column( 'PolicyName', String( 64 ), nullable = False ),
                               Column( 'Reason', String( 512 ), nullable = False, server_default = "Unspecified" ),
                               Column( 'Name', String( 64 ), nullable = False ),
                               Column( 'DateEffective', DateTime, nullable = False ),
                               Column( 'StatusType', String( 16 ), nullable = False, server_default = "" ),
                               Column( 'ID', Integer, nullable = False, autoincrement= True, primary_key = True ),
                               Column( 'LastCheckTime', DateTime, nullable = False ),
                               Column( 'Element', String( 32 ), nullable = False ),
                               mysql_engine = 'InnoDB' )

    self.log = gLogger.getSubLogger( 'ResourceManagementDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'ResourceStatus/ResourceManagementDB' )

    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                 echo = runDebug )

    self.metadata.bind = self.engine

    self.metadataTables = set()

    self.DBSession = sessionmaker( bind = self.engine )
    self.session = self.DBSession()

    # Create tables if they are not already created
    self.createTables()

  def _checkTable( self ):
    """ backward compatibility """
    self.createTables()

  def createTables( self ):
    """ create tables """

    try:
      self.metadata.create_all( self.engine )
    except exc.SQLAlchemyError as e:
      self.log.exception( "createTables: unexpected exception", lException = e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()

 # SQL Methods ###############################################################

  def insert( self, table, **kwargs ):
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

    try:

      table = self.metadata.tables.get( table ).insert()
      self.engine.execute( table, **kwargs )

      self.session.commit()

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "insert: unexpected exception", lException = e )
      return S_ERROR( "insert: unexpected exception %s" % e )

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

    try:

      # refresh metadata
      self.session.commit()

      table = self.metadata.tables.get( table )

      args = primaryKeystoList(table, **kwargs)

      result = self.session.query( table ).filter(*args)

      arr = []

      for u in result:
        rel = []
        for j in u:
         rel.append(j)

        arr.append(rel)

      return S_OK( arr )

    except exc.SQLAlchemyError as e:
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )

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

    try:

      meta = False

      # refresh metadata
      self.session.commit()

      table = self.metadata.tables.get( table )

      args = toList(table, **kwargs)

      columns = []
      for name, argument in kwargs.items():
        if argument and name == "Meta":
          meta = True
          for column in argument['columns']:
            columns.append( getattr(table.c, column) )

      if meta:
        result = self.session.execute( select( columns )
                                      .where( and_(*args) )
                                     )
      else :
        result = self.session.query( table ).filter(*args)

      arr = []

      for u in result:
        rel = []
        for j in u:
         rel.append(j)

        arr.append(rel)

      return S_OK( arr )

    except exc.SQLAlchemyError as e:
      self.log.exception( "select: unexpected exception", lException = e )
      return S_ERROR( "select: unexpected exception %s" % e )

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

    try:

      table = self.metadata.tables.get( table )

      args = primaryKeystoList(table, **kwargs)

      # fields to be updated
      params = toDict( **kwargs )

      self.session.execute( update( table )
                            .where( and_(*args) )
                            .values( **params )
                          )

      self.session.commit()

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "update: unexpected exception", lException = e )
      return S_ERROR( "update: unexpected exception %s" % e )

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

    try:

      table = self.metadata.tables.get( table )

      args = toList(table, **kwargs)

      self.session.execute( delete( table )
                            .where( or_(*args) )
                          )

      self.session.commit()

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "delete: unexpected exception", lException = e )
      return S_ERROR( "delete: unexpected exception %s" % e )

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

      if not result['Value']:
        self.insert( table, **kwargs )

    except exc.SQLAlchemyError as e:
      self.log.exception( "addIfNotThere: unexpected exception", lException = e )
      return S_ERROR( "addIfNotThere: unexpected exception %s" % e )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
