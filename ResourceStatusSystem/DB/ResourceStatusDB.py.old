''' ResourceStatusDB

  Module that provides basic methods to access the ResourceStatusDB.

'''

from datetime import datetime, timedelta

from DIRAC                                                 import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities            import getDBParameters

from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection                         import inspect
from sqlalchemy import create_engine, Table, Column, MetaData, String, DateTime, BigInteger, exc
from sqlalchemy.sql import update, select, delete, and_, or_


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


# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

def generateElementStatus(name):

  # Description of the ElementStatus table

  Table( name, metadata,
         Column( 'Status', String( 8 ), nullable = False, server_default = '' ),
         Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' ),
         Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
         Column( 'DateEffective', DateTime, nullable = False ),
         Column( 'TokenExpiration', String( 255 ), nullable = False , server_default = '9999-12-31 23:59:59' ),
         Column( 'ElementType', String( 32 ), nullable = False, server_default = '' ),
         Column( 'StatusType', String( 128 ), nullable = False, server_default = 'all', primary_key = True ),
         Column( 'LastCheckTime', DateTime, nullable = False , server_default = '1000-01-01 00:00:00' ),
         Column( 'TokenOwner', String( 16 ), nullable = False , server_default = 'rs_svc'),
         mysql_engine = 'InnoDB' )


def generateElementWithID(name):

  # Description of the ElementWithID table

  Table( name, metadata,
         Column( 'Status', String( 8 ), nullable = False, server_default = '' ),
         Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' ),
         Column( 'Name', String( 64 ), nullable = False ),
         Column( 'DateEffective', DateTime, nullable = False ),
         Column( 'TokenExpiration', String( 255 ), nullable = False , server_default = '9999-12-31 23:59:59' ),
         Column( 'ElementType', String( 32 ), nullable = False, server_default = '' ),
         Column( 'StatusType', String( 128 ), nullable = False, server_default = 'all' ),
         Column( 'ID', BigInteger, nullable = False, autoincrement= True, primary_key = True ),
         Column( 'LastCheckTime', DateTime, nullable = False , server_default = '1000-01-01 00:00:00' ),
         Column( 'TokenOwner', String( 16 ), nullable = False , server_default = 'rs_svc'),
         mysql_engine = 'InnoDB' )


class ResourceStatusDB( object ):
  """
      Collect from the CS all the info needed to connect to the DB.
  """

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

    self.log = gLogger.getSubLogger( 'ResourceStatusDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'ResourceStatus/ResourceStatusDB' )

    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser,
                                                              self.dbPass,
                                                              self.dbHost,
                                                              self.dbPort,
                                                              self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine

    self.metadataTables = set()
    self.elementWithIDTables = [ 'SiteLog', 'SiteHistory', 'ResourceLog', 'ResourceHistory',
                                 'NodeLog', 'NodeHistory', 'ComponentLog', 'ComponentHistory' ]

    self.DBSession = sessionmaker( bind = self.engine )

  def createTables( self ):
    """ create tables """

    try:
      for names in [ 'SiteStatus', 'ResourceStatus', 'NodeStatus', 'ComponentStatus' ]:
        if names not in self.metadataTables:
          generateElementStatus(names)
          self.metadataTables.add(names)

      for names in self.elementWithIDTables:
        if names not in self.metadataTables:
          generateElementWithID(names)
          self.metadataTables.add(names)

      metadata.create_all( self.engine )

    except exc.SQLAlchemyError as e:
      self.log.exception( "createTables: unexpected exception", lException = e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()

  # SQL Methods ###############################################################

  def selectPrimaryKeys( self, element, tableType, name = None, statusType = None,
                         status = None, elementType = None, reason = None,
                         dateEffective = None, lastCheckTime = None,
                         tokenOwner = None, tokenExpiration = None ):

    session = self.DBSession()

    try:

      table = metadata.tables.get( element + tableType )

      args = primaryKeystoList( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                                Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                                TokenOwner = tokenOwner, TokenExpiration = tokenExpiration)

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

  def select( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None, meta = None ):

    session = self.DBSession()

    try:

      table = metadata.tables.get( element + tableType )

      args = toList( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                     Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                     TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      # this is the variable where we store the column names that correspond to the values that we are going to return
      columnNames = []

      # if meta['columns'] is specified select only these columns
      if meta and 'columns' in meta:
        columns = []
        for column in meta['columns']:
          columns.append( getattr(table.c, column) )
          columnNames.append( column )

        result = session.execute( select( columns )
                                  .where( and_(*args) ) )

      else:
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

  def insert(self, element, tableType, name, statusType, status,
             elementType, reason, dateEffective, lastCheckTime,
             tokenOwner, tokenExpiration ):

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.DBSession( expire_on_commit = False )

    try:

      # defaults
      if not dateEffective:
        dateEffective = datetime.utcnow()
      if not lastCheckTime:
        lastCheckTime = datetime.utcnow()
      if not tokenExpiration:
        tokenExpiration = datetime.utcnow() + timedelta(hours=24)

      table = metadata.tables.get( element + tableType ).insert()
      self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                           Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                           TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      session.commit()
      session.expunge_all()

      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "insert: unexpected exception", lException = e )
      return S_ERROR( "insert: unexpected exception %s" % e )
    finally:
      session.close()

  def update( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None, ID = None ):

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.DBSession( expire_on_commit = False )

    try:

      table = metadata.tables.get( element + tableType )

      # fields to be selected (primary keys)
      if table in self.elementWithIDTables:
        args = toList(table, ID = ID)
      else:
        args = toList(table, Name = name, StatusType = statusType)

      # fields to be updated
      params = toDict( Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                       Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                       TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      session.execute( update( table )
                       .where( and_(*args) )
                       .values( **params )
                     )

      session.commit()
      session.expunge_all()

      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "update: unexpected exception", lException = e )
      return S_ERROR( "update: unexpected exception %s" % e )
    finally:
      session.close()

  def delete( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None, meta = None ):

    session = self.DBSession()

    try:

      table = metadata.tables.get( element + tableType )

      args = toList(table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                    Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                    TokenOwner = tokenOwner, TokenExpiration = tokenExpiration, Meta = meta)

      session.execute( delete( table ).where( or_(*args) ) )

      session.commit()

      return S_OK()

    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception( "delete: unexpected exception", lException = e )
      return S_ERROR( "delete: unexpected exception %s" % e )
    finally:
      session.close()

  # Extended SQL methods ######################################################

  def modify( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None, log = None ):

    session = self.DBSession()

    self.update( element, tableType, name, statusType, status, elementType, reason ,
                 dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if log:
      try:

        table = metadata.tables.get( element + 'Log' ).insert()
        self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                             Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                             TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

        session.commit()
        session.expunge_all()

      except exc.SQLAlchemyError as e:
        self.log.exception( "modify: unexpected exception", lException = e )
        return S_ERROR( "modify: unexpected exception %s" % e )
      finally:
        session.close()

    return S_OK()

  def addIfNotThere( self, element, tableType, name, statusType,
                     status, elementType, reason,
                     dateEffective, lastCheckTime,
                     tokenOwner, tokenExpiration, log = None ):

    session = self.DBSession()

    result = self.select( element, tableType, name, statusType, status, elementType, reason ,
                          dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if not result['OK']:
      return result

    if not result['Value']:
      self.insert( element, tableType, name, statusType, status, elementType, reason ,
                   dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if log:
      try:

        table = metadata.tables.get( element + 'Log' ).insert()
        self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                             Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                             TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

        session.commit()
        session.expunge_all()

      except exc.SQLAlchemyError as e:
        self.log.exception( "addIfNotThere: unexpected exception", lException = e )
        return S_ERROR( "addIfNotThere: unexpected exception %s" % e )
      finally:
        session.close()

    return S_OK()

  def addOrModify( self, element, tableType, name = None, statusType = None,
                   status = None, elementType = None, reason = None,
                   dateEffective = None, lastCheckTime = None,
                   tokenOwner = None, tokenExpiration = None, log = None ):
    try:

      result = self.selectPrimaryKeys( element, tableType, name, statusType, status, elementType, reason ,
                                       dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

      if not result['OK']:
        return result

      if not result['Value']:
        self.insert( element, tableType, name, statusType, status, elementType, reason ,
                     dateEffective, lastCheckTime, tokenOwner, tokenExpiration )
      else:
        self.modify( element, tableType, name, statusType, status, elementType, reason ,
                     dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

      if log:

        table = metadata.tables.get( element + 'Log' ).insert()
        self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                             Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                             TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

    except exc.SQLAlchemyError as e:
      self.log.exception( "addOrModify: unexpected exception", lException = e )
      return S_ERROR( "addOrModify: unexpected exception %s" % e )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
