''' ResourceStatusDB

  Module that provides basic methods to access the ResourceStatusDB.

'''

__RCSID__ = "$Id$"


import datetime

from DIRAC                                                 import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities            import getDBParameters

from sqlalchemy.orm import sessionmaker, class_mapper
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, DateTime, exc, BigInteger

# Defining the tables

rssBase = declarative_base()

class ElementStatusBase(object):
  """ Prototype for tables
  """

  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column( 'Name', String( 64 ), nullable = False, primary_key = True )
  statustype = Column( 'StatusType', String( 128 ), nullable = False, server_default = 'all', primary_key = True )
  status = Column( 'Status', String( 8 ), nullable = False, server_default = '' )
  reason = Column( 'Reason', String( 512 ), nullable = False, server_default = 'Unspecified' )
  dateeffective = Column( 'DateEffective', DateTime, nullable = False )
  tokenexpiration = Column( 'TokenExpiration', String( 255 ), nullable = False ,
                            server_default = '9999-12-31 23:59:59' )
  elementtype = Column( 'ElementType', String( 32 ), nullable = False, server_default = '' )
  lastchecktime = Column( 'LastCheckTime', DateTime, nullable = False , server_default = '1000-01-01 00:00:00' )
  tokenowner = Column( 'TokenOwner', String( 16 ), nullable = False , server_default = 'rs_svc')

  def fromDict( self, dictionary ):
    """
    Fill the fields of the AccountingCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.name = dictionary.get( 'Name', self.name )
    self.statustype = dictionary.get( 'StatusType', self.statustype )
    self.status = dictionary.get( 'Status', self.status )
    self.reason = dictionary.get( 'Reason', self.reason )
    self.dateeffective = dictionary.get( 'DateEffective', self.dateeffective )
    self.tokenexpiration = dictionary.get( 'TokenExpiration', self.tokenexpiration )
    self.elementtype = dictionary.get( 'ElementType', self.elementtype )
    self.lastchecktime = dictionary.get( 'LastCheckTime', self.lastchecktime )
    self.tokenowner = dictionary.get( 'TokenOwner', self.tokenowner )

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.name, self.statustype, self.status, self.reason,
            self.dateeffective, self.tokenexpiration, self.elementtype,
            self.lastchecktime, self.tokenowner]


class ElementStatusBaseWithID(ElementStatusBase):
  """ Prototype for tables
  """

  id = Column( 'ID', BigInteger, nullable = False, autoincrement= True, primary_key = True )

  def fromDict( self, dictionary ):
    """
    Fill the fields of the AccountingCache object from a dictionary

    :param dictionary: Dictionary to fill a single line
    :type arguments: dict
    """

    self.id = dictionary.get( 'ID', self.id )

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.id, self.name, self.statustype, self.status, self.reason,
            self.dateeffective, self.tokenexpiration, self.elementtype,
            self.lastchecktime, self.tokenowner]


### tables with schema defined in ElementStatusBase

class SiteStatus(ElementStatusBase, rssBase):
  """ SiteStatus table
  """

  __tablename__ = 'SiteStatus'

class ElementStatus(ElementStatusBase, rssBase):
  """ ElementStatus table
  """

  __tablename__ = 'ElementStatus'

class NodeStatus(ElementStatusBase, rssBase):
  """ NodeStatus table
  """

  __tablename__ = 'NodeStatus'

class ComponentStatus(ElementStatusBase, rssBase):
  """ ComponentStatus table
  """

  __tablename__ = 'ComponentStatus'




### tables with schema defined in ElementStatusBaseWithID

class SiteLog(ElementStatusBaseWithID, rssBase):
  """ SiteLog table
  """

  __tablename__ = 'SiteLog'

class SiteHistory(ElementStatusBaseWithID, rssBase):
  """ SiteHistory table
  """
  __tablename__ = 'SiteHistory'


class ElementLog(ElementStatusBaseWithID, rssBase):
  """ ElementLog table
  """

  __tablename__ = 'ElementLog'

class ElementHistory(ElementStatusBaseWithID, rssBase):
  """ ElementHistory table
  """
  __tablename__ = 'ElementHistory'


class NodeLog(ElementStatusBaseWithID, rssBase):
  """ NodeLog table
  """

  __tablename__ = 'NodeLog'

class NodeHistory(ElementStatusBaseWithID, rssBase):
  """ NodeHistory table
  """
  __tablename__ = 'NodeHistory'


class ComponentLog(ElementStatusBaseWithID, rssBase):
  """ ComponentLog table
  """

  __tablename__ = 'ComponentLog'

class ComponentHistory(ElementStatusBaseWithID, rssBase):
  """ ComponentHistory table
  """
  __tablename__ = 'ComponentHistory'



### Interaction with the DB

class ResourceStatusDB( object ):
  '''
    Class that defines the tables for the ResourceStatusDB on a python dictionary.
  '''

  def __init__( self ):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'ResourceStatusDB' )

    self.__initializeConnection( 'ResourceStatus/ResourceStatusDB' )
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

    for table in ['SiteStatus',
                  'ElementStatus',
                  'NodeStatus',
                  'ComponentStatus']:
      if table not in tablesInDB:
        getattr(__import__(__name__, globals(), locals(), [table]), table).__table__.create( self.engine ) #pylint: disable=no-member
      else:
        gLogger.debug( 'Table \'%s\' already exists' %table )

    for table in ['SiteLog',
                  'SiteHistory',
                  'ElementLog',
                  'ElementHistory',
                  'NodeLog',
                  'NodeHistory',
                  'ComponentLog',
                  'ComponentHistory']:
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
        if columnName.lower() == 'meta': # special case
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
