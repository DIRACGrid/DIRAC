''' ResourceStatusDB

  Module that provides basic methods to access the ResourceStatusDB.

'''

import datetime

from DIRAC                                                 import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities            import getDBParameters

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, Column, MetaData, String, DateTime, BigInteger, exc
from sqlalchemy.sql import update, select

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
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine

    self.metadataTables = set()

    self.DBSession = sessionmaker( bind = self.engine )
    self.session = self.DBSession()

    # Create tables if they are not already created
    self.createTables()

  def createTables( self ):
    """ create tables """

    ElementStatusTables = [ 'SiteStatus', 'ResourceStatus',
                            'NodeStatus', 'ComponentStatus' ]

    ElementWithIDTables = [ 'SiteLog', 'SiteHistory', 'ResourceLog', 'ResourceHistory',
                            'NodeLog', 'NodeHistory', 'ComponentLog', 'ComponentHistory' ]

    try:

      for names in ElementStatusTables:
        if names not in self.metadataTables:
          generateElementStatus(names)
          self.metadataTables.add(names)

      for names in ElementWithIDTables:
        if names not in self.metadataTables:
          generateElementWithID(names)
          self.metadataTables.add(names)

      metadata.create_all( self.engine )

    except exc.SQLAlchemyError as e:
      self.log.exception( "createTables: unexpected exception", lException = e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()

  # SQL Methods ###############################################################

  def insert(self, element, tableType, name, statusType, status,
             elementType, reason, dateEffective, lastCheckTime,
             tokenOwner, tokenExpiration ):

    try:

      table = metadata.tables.get( element + tableType ).insert()
      self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                           Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                           TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "insert: unexpected exception", lException = e )
      return S_ERROR( "insert: unexpected exception %s" % e )

  def select( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None ):

    try:

      table = metadata.tables.get( element + tableType )

      filters = []

      if name:
        filters.append(table.c.Name == name)
      if statusType:
        filters.append(table.c.StatusType == statusType)
      if status:
        filters.append(table.c.Status == status)
      if elementType:
        filters.append(table.c.ElementType == elementType)
      if reason:
        filters.append(table.c.Reason == reason)
      if dateEffective:
        filters.append(table.c.DateEffective == dateEffective)
      if lastCheckTime:
        filters.append(table.c.LastCheckTime == lastCheckTime)
      if tokenOwner:
        filters.append(table.c.TokenOwner == tokenOwner)
      if tokenExpiration:
        filters.append(table.c.TokenExpiration == tokenExpiration)

      result = self.session.query( table ).filter(*filters)

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

  def update( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None ):

    try:

      table = metadata.tables.get( element + tableType )

      filters = []

      # fields to be selected (primary keys)
      if name:
        filters.append(table.c.Name == name)
      elif statusType:
        filters.append(table.c.StatusType == statusType)

      # fields to be updated
      params = {}

      if name:
        params.update( {"Name" : name} )
      if statusType:
        params.update( {"StatusType" : statusType} )
      if status:
        params.update( {"Status" : status} )
      if elementType:
        params.update( {"ElementType" : elementType} )
      if reason:
        params.update( {"Reason" : reason} )
      if dateEffective:
        params.update( {"DateEffective" : dateEffective} )
      if lastCheckTime:
        params.update( {"LastCheckTime" : lastCheckTime} )
      if tokenOwner:
        params.update( {"TokenOwner" : tokenOwner} )
      if tokenExpiration:
        params.update( {"TokenExpiration" : tokenExpiration} )

      self.session.execute( update( table )
                            .where( *filters )
                            .values( **params )
                          )

      self.session.commit()

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "update: unexpected exception", lException = e )
      return S_ERROR( "update: unexpected exception %s" % e )

  def delete( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None ):

    try:

      table = metadata.tables.get( element + tableType ).delete()
      self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                           Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                           TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      return S_OK()

    except exc.SQLAlchemyError as e:
      self.log.exception( "delete: unexpected exception", lException = e )
      return S_ERROR( "delete: unexpected exception %s" % e )

  # Extended SQL methods ######################################################

  def modify( self, element, tableType, name = None, statusType = None,
              status = None, elementType = None, reason = None,
              dateEffective = None, lastCheckTime = None,
              tokenOwner = None, tokenExpiration = None, log = None ):

    self.update( element, tableType, name, statusType, status, elementType, reason ,
                 dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if log:
      try:

        table = metadata.tables.get( element + 'Log' ).insert()
        self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                             Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                             TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      except exc.SQLAlchemyError as e:
        self.log.exception( "modify: unexpected exception", lException = e )
        return S_ERROR( "modify: unexpected exception %s" % e )

    return S_OK()

  def addIfNotThere( self, element, tableType, name = None, statusType = None,
                     status = None, elementType = None, reason = None,
                     dateEffective = None, lastCheckTime = None,
                     tokenOwner = None, tokenExpiration = None, log = None ):

    result = self.select( element, tableType, name, statusType, status, elementType, reason ,
                          dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if not result['Value']:
      self.insert( element, tableType, name, statusType, status, elementType, reason ,
                   dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if log:
      try:

        table = metadata.tables.get( element + 'Log' ).insert()
        self.engine.execute( table, Name = name, StatusType = statusType, Status = status, ElementType = elementType,
                             Reason = reason, DateEffective = dateEffective, LastCheckTime = lastCheckTime,
                             TokenOwner = tokenOwner, TokenExpiration = tokenExpiration )

      except exc.SQLAlchemyError as e:
        self.log.exception( "addIfNotThere: unexpected exception", lException = e )
        return S_ERROR( "addIfNotThere: unexpected exception %s" % e )

    return S_OK()

  def addOrModify( self, element, tableType, name = None, statusType = None,
                   status = None, elementType = None, reason = None,
                   dateEffective = None, lastCheckTime = None,
                   tokenOwner = None, tokenExpiration = None, log = None ):

    result = self.select( element, tableType, name, statusType, status, elementType, reason ,
                          dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if not result['Value']:
      self.insert( element, tableType, name, statusType, status, elementType, reason ,
                   dateEffective, lastCheckTime, tokenOwner, tokenExpiration )
    else:
      self.modify( element, tableType, name, statusType, status, elementType, reason ,
                   dateEffective, lastCheckTime, tokenOwner, tokenExpiration )

    if log:
      try:

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
