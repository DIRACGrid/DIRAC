########################################################################
# $HeadURL$
########################################################################
""" DIRAC Basic MySQL Class
    It provides access to the basic MySQL methods in a multithread-safe mode
    keeping used connections in a python Queue for further reuse.

    These are the coded methods:


    __init__( host, user, passwd, name, [maxConnsInQueue=10] )

    Initializes the Queue and tries to connect to the DB server,
    using the _connect method.
    "maxConnsInQueue" defines the size of the Queue of open connections
    that are kept for reuse. It also defined the maximum number of open
    connections available from the object.
    maxConnsInQueue = 0 means unlimited and it is not supported.


    _except( methodName, exception, errorMessage )

    Helper method for exceptions: the "methodName" and the "errorMessage"
    are printed with ERROR level, then the "exception" is printed (with
    full description if it is a MySQL Exception) and S_ERROR is returned
    with the errorMessage and the exception.


    _connect()

    Attempts connection to DB and sets the _connected flag to True upon success.
    Returns S_OK or S_ERROR.


    _query( cmd, [conn] )

    Executes SQL command "cmd".
    Gets a connection from the Queue (or open a new one if none is available),
    the used connection is  back into the Queue.
    If a connection to the the DB is passed as second argument this connection
    is used and is not  in the Queue.
    Returns S_OK with fetchall() out in Value or S_ERROR upon failure.


    _update( cmd, [conn] )

    Executes SQL command "cmd" and issue a commit
    Gets a connection from the Queue (or open a new one if none is available),
    the used connection is  back into the Queue.
    If a connection to the the DB is passed as second argument this connection
    is used and is not  in the Queue
    Returns S_OK with number of updated registers in Value or S_ERROR upon failure.


    _createTables( tableDict )

    Create a new Table in the DB


    _getConnection()

    Gets a connection from the Queue (or open a new one if none is available)
    Returns S_OK with connection in Value or S_ERROR
    the calling method is responsible for closing this connection once it is no
    longer needed.




    Some high level methods have been added to avoid the need to write SQL
    statement in most common cases. They should be used instead of low level
    _insert, _update methods when ever possible.

    buildCondition( self, condDict = None, older = None, newer = None,
                      timeStamp = None, orderAttribute = None, limit = False,
                      greater = None, smaller = None ):

      Build SQL condition statement from provided condDict and other extra check on
      a specified time stamp.
      The conditions dictionary specifies for each attribute one or a List of possible
      values
      greater and smaller are dictionaries in which the keys are the names of the fields,
      that are requested to be >= or < than the corresponding value.
      For compatibility with current usage it uses Exceptions to exit in case of
      invalid arguments


    insertFields( self, tableName, inFields = None, inValues = None, conn = None, inDict = None ):

      Insert a new row in "tableName" assigning the values "inValues" to the
      fields "inFields".
      Alternatively inDict can be used
      String type values will be appropriately escaped.


    updateFields( self, tableName, updateFields = None, updateValues = None,
                  condDict = None,
                  limit = False, conn = None,
                  updateDict = None,
                  older = None, newer = None,
                  timeStamp = None, orderAttribute = None ):

      Update "updateFields" from "tableName" with "updateValues".
      updateDict alternative way to provide the updateFields and updateValues
      N records can match the condition
      return S_OK( number of updated rows )
      if limit is not False, the given limit is set
      String type values will be appropriately escaped.


    deleteEntries( self, tableName,
                   condDict = None,
                   limit = False, conn = None,
                   older = None, newer = None,
                   timeStamp = None, orderAttribute = None ):

      Delete rows from "tableName" with
      N records can match the condition
      if limit is not False, the given limit is set
      String type values will be appropriately escaped, they can be single values or lists of values.


    getFields( self, tableName, outFields = None,
               condDict = None,
               limit = False, conn = None,
               older = None, newer = None,
               timeStamp = None, orderAttribute = None ):

      Select "outFields" from "tableName" with condDict
      N records can match the condition
      return S_OK( tuple(Field,Value) )
      if limit is not False, the given limit is set
      String type values will be appropriately escaped, they can be single values or lists of values.

      for compatibility with other methods condDict keyed argument is added


    getCounters( self, table, attrList, condDict = None, older = None,
                 newer = None, timeStamp = None, connection = False ):

      Count the number of records on each distinct combination of AttrList, selected
      with condition defined by condDict and time stamps


    getDistinctAttributeValues( self, table, attribute, condDict = None, older = None,
                                newer = None, timeStamp = None, connection = False ):

      Get distinct values of a table attribute under specified conditions


"""

__RCSID__ = "$Id$"


from DIRAC                                  import gLogger
from DIRAC                                  import S_OK, S_ERROR
from DIRAC.Core.Utilities.DataStructures    import MutableStruct
from DIRAC.Core.Utilities                   import Time

# Get rid of the annoying Deprecation warning of the current MySQLdb
# FIXME: compile a newer MySQLdb version
import warnings
with warnings.catch_warnings():
  warnings.simplefilter( 'ignore', DeprecationWarning )
  import MySQLdb

# Get rid of the annoying Deprecation warning of the current MySQLdb
# FIXME: compile a newer MySQLdb version
import warnings
with warnings.catch_warnings():
  warnings.simplefilter( 'ignore', DeprecationWarning )
  import MySQLdb
  
# This is for proper initialization of embeded server, it should only be called once
MySQLdb.server_init( ['--defaults-file=/opt/dirac/etc/my.cnf', '--datadir=/opt/mysql/db'], ['mysqld'] )
gInstancesCount = 0
gDebugFile = None

import collections
import time
import threading
from types import StringTypes, DictType, ListType, TupleType

MAXCONNECTRETRY = 10

def _checkQueueSize( maxQueueSize ):
  """
    Helper to check maxQueueSize
  """
  if maxQueueSize <= 0:
    raise Exception( 'MySQL.__init__: maxQueueSize must positive' )
  try:
    maxQueueSize - 1
  except Exception:
    raise Exception( 'MySQL.__init__: wrong type for maxQueueSize' )

def _checkFields( inFields, inValues ):
  """
    Helper to check match between inFields and inValues lengths
  """

  if inFields == None and inValues == None:
    return S_OK()

  try:
    assert len( inFields ) == len( inValues )
  except:
    return S_ERROR( 'Mismatch between inFields and inValues.' )

  return S_OK()

def _quotedList( fieldList = None ):
  """
    Quote a list of MySQL Field Names with "`"
    Return a comma separated list of quoted Field Names

    To be use for Table and Field Names
  """
  if fieldList == None:
    return None
  quotedFields = []
  try:
    for field in fieldList:
      quotedFields.append( '`%s`' % field.replace( '`', '' ) )
  except Exception:
    return None
  if not quotedFields:
    return None

  return ', '.join( quotedFields )


class MySQL:
  """
  Basic multithreaded DIRAC MySQL Client Class
  """
  __initialized = False

  class ConnectionPool( object ):
    """
    Management of connections per thread
    """
    __connData = MutableStruct( 'ConnData', [ 'conn', 'dbName', 'last', 'intrans' ] )

    def __init__( self, host, user, passwd, port = 3306, graceTime = 600 ):
      self.__host = host
      self.__user = user
      self.__passwd = passwd
      self.__port = port
      self.__graceTime = graceTime
      self.__spares = collections.deque()
      self.__maxSpares = 10
      self.__lastClean = 0
      self.__assigned = {}

    @property
    def __thid( self ):
      return threading.current_thread()

    def __newConn( self ):
      conn = MySQLdb.connect( host = self.__host,
                              port = self.__port,
                              user = self.__user,
                              passwd = self.__passwd )

      self.__execute( conn, "SET AUTOCOMMIT=1" )
      return conn

    def __execute( self, conn, cmd ):
      cursor = conn.cursor()
      res = cursor.execute( cmd )
      cursor.close()
      return res

    def get( self, dbName, retries = 10 ):
      retries = max( 0, min( MAXCONNECTRETRY, retries ) )
      self.clean()
      result = self.__getWithRetry( dbName, retries, retries )
      if not result[ 'OK' ]:
        return result
      return S_OK( result[ 'Value' ].conn )

    def __getWithRetry( self, dbName, totalRetries = 10, retriesLeft = 10 ):
      sleepTime = 5 * ( totalRetries - retriesLeft )
      if sleepTime > 0:
        time.sleep( sleepTime )
      try:
        connData, thid = self.__innerGet()
      except MySQLdb.MySQLError, excp:
        if retriesLeft >= 0:
          return self.__getWithRetry( dbName, totalRetries, retriesLeft - 1 )
        return S_ERROR( "Could not connect: %s" % excp )

      if not connData.intrans and not self.__ping( connData.conn ):
        try:
          self.__assigned.pop( thid )
        except KeyError:
          pass
        if retriesLeft >= 0:
          return self.__getWithRetry( dbName, totalRetries, retriesLeft )
        return S_ERROR( "Could not connect" )

      if connData.dbName != dbName:
        try:
          connData.conn.select_db( dbName )
          connData.dbName = dbName
        except MySQLdb.MySQLError, excp:
          try:
            self.__assigned.pop( thid ).conn.close()
          except Exception:
            pass
          if retriesLeft >= 0:
            return self.__getWithRetry( dbName, totalRetries, retriesLeft - 1 )
          return S_ERROR( "Could not select db %s: %s" % ( dbName, excp ) )
      return S_OK( connData )

    def __ping( self, conn ):
      try:
        conn.ping( True )
        return True
      except:
        return False

    def __innerGet( self ):
      thid = self.__thid
      now = time.time()
      try:
        data = self.__assigned[ thid ]
        data.last = now
        return data, thid
      except KeyError:
        pass
      #Not cached
      try:
        connData = self.__spares.pop()
      except IndexError:
        connData = self.__connData( self.__newConn(), "", now, False )

      self.__assigned[ thid ] = connData
      return self.__assigned[ thid ], thid

    def __pop( self, thid ):
      try:
        connData = self.__assigned.pop( thid )
      except KeyError:
        return
      if not connData.intrans and len( self.__spares ) < self.__maxSpares:
        self.__spares.append( connData )
      else:
        connData.conn.close()

    def clean( self, now = False ):
      if not now:
        now = time.time()
      self.__lastClean = now
      for thid in list( self.__assigned ):
        if not thid.isAlive():
          self.__pop( thid )
          continue
        try:
          data = self.__assigned[ thid ]
        except KeyError:
          continue
        if now - data.last > self.__graceTime:
          self.__pop( thid )

    def transactionStart( self, dbName ):
      print "TRANS START"
      result = self.__getWithRetry( dbName )
      if not result[ 'OK' ]:
        return result
      connData = result[ 'Value' ]
      try:
        if connData.intrans:
          raise RuntimeError( "Staring a MySQL transaction inside another one" )
      	self.__execute( connData.conn, "SET AUTOCOMMIT=0" )
        self.__execute( connData.conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT" )
        connData.intrans = True
        return S_OK()
      except MySQLdb.MySQLError, excp:
        return S_ERROR( "Could not begin transaction: %s" % excp )

    def transactionCommit( self, dbName ):
      print "TRANS COMMIT"
      return self.__endTransaction( dbName, True )

    def transactionRollback( self, dbName ):
      print "TRANS ROLLBACK"
      return self.__endTransaction( dbName, False )

    def __endTransaction( self, dbName, commit ):
      result = self.__getWithRetry( dbName )
      if not result[ 'OK' ]:
        return result
      connData = result[ 'Value' ]
      try:
        if not connData.intrans:
          gLogger.warn( "MySQL connection has reconnected. Transaction may be inconsistent" )
        if commit:
          result = connData.conn.commit()
        else:
          result = connData.conn.rollback()
      	self.__execute( connData.conn, "SET AUTOCOMMIT=1" )
        connData.conn.commit()
        connData.intrans = False
        return S_OK( result )
      except MySQLdb.MySQLError, excp:
        return S_ERROR( "Could not end transaction: %s" % excp )

  __connectionPools = {}

  def __init__( self, hostName, userName, passwd, dbName, port = 3306, maxQueueSize = 3, debug = False ):
    """
    set MySQL connection parameters and try to connect
    """
    global gInstancesCount, gDebugFile
    gInstancesCount += 1

    self._connected = False

    if 'log' not in dir( self ):
      self.log = gLogger.getSubLogger( 'MySQL' )
    self.logger = self.log

    # let the derived class decide what to do with if is not 1
    self._threadsafe = MySQLdb.thread_safe()
    self.log.debug( 'thread_safe = %s' % self._threadsafe )

    _checkQueueSize( maxQueueSize )

    self.__hostName = str( hostName )
    self.__userName = str( userName )
    self.__passwd = str( passwd )
    self.__dbName = str( dbName )
    self.__port = port
    cKey = ( self.__hostName, self.__userName, self.__passwd, self.__port )
    if cKey not in MySQL.__connectionPools:
      MySQL.__connectionPools[ cKey ] = MySQL.ConnectionPool( *cKey )
    self.__connectionPool = MySQL.__connectionPools[ cKey ]

    self.__initialized = True
    result = self._connect()
    if not result[ 'OK' ]:
      gLogger.error( "Cannot connect to to DB: %s" % result[ 'Message' ] )

    if debug:
      try:
        gDebugFile = open( "%s.debug.log" % self.__dbName, "w" )
      except IOError:
        pass


  def __del__( self ):
    global gInstancesCount
    try:
      gInstancesCount -= 1
    except Exception:
      pass

  def _except( self, methodName, x, err ):
    """
    print MySQL error or exception
    return S_ERROR with Exception
    """

    try:
      raise x
    except MySQLdb.Error, e:
      self.log.debug( '%s: %s' % ( methodName, err ),
                     '%d: %s' % ( e.args[0], e.args[1] ) )
      return S_ERROR( '%s: ( %d: %s )' % ( err, e.args[0], e.args[1] ) )
    except Exception, e:
      self.log.debug( '%s: %s' % ( methodName, err ), str( e ) )
      return S_ERROR( '%s: (%s)' % ( err, str( e ) ) )


  def __escapeString( self, myString ):
    """
    To be used for escaping any MySQL string before passing it to the DB
    this should prevent passing non-MySQL accepted characters to the DB
    It also includes quotation marks " around the given string
    """

    retDict = self.__getConnection()
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    specialValues = ( 'UTC_TIMESTAMP', 'TIMESTAMPADD', 'TIMESTAMPDIFF' )

    try:
      myString = str( myString )
    except ValueError:
      return S_ERROR( "Cannot escape value!" )

    try:
      for sV in specialValues:
        if myString.find( sV ) == 0:
          return S_OK( myString )
      escape_string = connection.escape_string( str( myString ) )
      self.log.debug( '__escape_string: returns', '"%s"' % escape_string )
      return S_OK( '"%s"' % escape_string )
    except Exception, x:
      self.log.debug( '__escape_string: Could not escape string', '"%s"' % myString )
      return self._except( '__escape_string', x, 'Could not escape string' )

  def __checkTable( self, tableName, force = False ):

    table = _quotedList( [tableName] )
    if not table:
      return S_ERROR( 'Invalid tableName argument' )

    cmd = 'SHOW TABLES'
    retDict = self._query( cmd, debug = True )
    if not retDict['OK']:
      return retDict
    if ( tableName, ) in retDict['Value']:
      if not force:
        # the requested exist and table creation is not force, return with error
        return S_ERROR( 'Requested table %s already exists' % tableName )
      else:
        cmd = 'DROP TABLE %s' % table
        retDict = self._update( cmd, debug = True )
        if not retDict['OK']:
          return retDict

    return S_OK()


  def _escapeString( self, myString, conn = None ):
    """
      Wrapper around the internal method __escapeString
    """
    self.log.debug( '_escapeString:', '"%s"' % str( myString ) )

    return self.__escapeString( myString )


  def _escapeValues( self, inValues = None ):
    """
    Escapes all strings in the list of values provided
    """
    self.log.debug( '_escapeValues:', inValues )

    inEscapeValues = []

    if not inValues:
      return S_OK( inEscapeValues )

    for value in inValues:
      if type( value ) in StringTypes:
        retDict = self.__escapeString( value )
        if not retDict['OK']:
          return retDict
        inEscapeValues.append( retDict['Value'] )
      elif type( value ) == TupleType or type( value ) == ListType:
        tupleValues = []
        for v in list( value ):
          retDict = self.__escapeString( v )
          if not retDict['OK']:
            return retDict
          tupleValues.append( retDict['Value'] )
        inEscapeValues.append( '(' + ', '.join( tupleValues ) + ')' )
      else:
        retDict = self.__escapeString( str( value ) )
        if not retDict['OK']:
          return retDict
        inEscapeValues.append( retDict['Value'] )
    return S_OK( inEscapeValues )


  def _connect( self ):
    """
    open connection to MySQL DB and put Connection into Queue
    set connected flag to True and return S_OK
    return S_ERROR upon failure
    """
    if not self.__initialized:
      error = 'DB not properly initialized'
      gLogger.error( error )
      return S_ERROR( error )

    self.log.debug( '_connect:', self._connected )
    if self._connected:
      return S_OK()

    self.log.debug( '_connect: Attempting to access DB',
                       '[%s@%s] by user %s/%s.' %
                       ( self.__dbName, self.__hostName, self.__userName, self.__passwd ) )
    try:
      self.log.verbose( '_connect: Connected.' )
      self._connected = True
      return S_OK()
    except Exception, x:
      return self._except( '_connect', x, 'Could not connect to DB.' )


  def _query( self, cmd, conn = None, debug = False ):
    """
    execute MySQL query command
    return S_OK structure with fetchall result as tuple
    it returns an empty tuple if no matching rows are found
    return S_ERROR upon error
    """
    if debug:
      self.logger.debug( '_query:', cmd )
    else:
      if self.logger._minLevel == self.logger._logLevels.getLevelValue( 'DEBUG' ):
        self.logger.verbose( '_query:', cmd )
      else:
        self.logger.verbose( '_query:', cmd[:min( len( cmd ) , 512 )] )

    if gDebugFile:
      start = time.time()

    retDict = self.__getConnection()
    if not retDict['OK']:
      return retDict
    connection = retDict[ 'Value' ]

    try:
      cursor = connection.cursor()
      if cursor.execute( cmd ):
        res = cursor.fetchall()
      else:
        res = ()

      # Log the result limiting it to just 10 records
      if len( res ) <= 10:
        if debug:
          self.logger.debug( '_query: returns', res )
        else:
          self.logger.verbose( '_query: returns', res )
      else:
        if debug:
          self.logger.debug( '_query: Total %d records returned' % len( res ) )
          self.logger.debug( '_query: %s ...' % str( res[:10] ) )
        else:
          self.logger.verbose( '_query: Total %d records returned' % len( res ) )
          self.logger.verbose( '_query: %s ...' % str( res[:10] ) )

      retDict = S_OK( res )
    except Exception , x:
      self.log.warn( '_query:', cmd )
      retDict = self._except( '_query', x, 'Execution failed.' )

    try:
      cursor.close()
    except Exception:
      pass

    if gDebugFile:
      print >> gDebugFile, time.time() - start, cmd.replace( '\n', '' )
      gDebugFile.flush()

    return retDict


  def _update( self, cmd, conn = None, debug = False ):
    """ execute MySQL update command
        return S_OK with number of updated registers upon success
        return S_ERROR upon error
    """
    if debug:
      self.logger.debug( '_update:', cmd )
    else:
      if self.logger._minLevel == self.logger._logLevels.getLevelValue( 'DEBUG' ):
        self.logger.verbose( '_update:', cmd )
      else:
        self.logger.verbose( '_update:', cmd[:min( len( cmd ) , 512 )] )

    if gDebugFile:
      start = time.time()

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      res = cursor.execute( cmd )
      # connection.commit()
      if debug:
        self.log.debug( '_update:', res )
      else:
        self.log.verbose( '_update:', res )
      retDict = S_OK( res )
      if cursor.lastrowid:
        retDict[ 'lastRowId' ] = cursor.lastrowid
    except Exception, x:
      self.log.warn( '_update: %s: %s' % ( cmd, str( x ) ) )
      retDict = self._except( '_update', x, 'Execution failed.' )

    try:
      cursor.close()
    except Exception:
      pass

    if gDebugFile:
      print >> gDebugFile, time.time() - start, cmd.replace( '\n', '' )
      gDebugFile.flush()

    return retDict

  def _transaction( self, cmdList, conn = None ):
    """ dummy transaction support

    :param self: self reference
    :param list cmdList: list of queries to be executed within the transaction
    :param MySQLDB.Connection conn: connection

    :return: S_OK( [ ( cmd1, ret1 ), ... ] ) or S_ERROR
    """
    if type( cmdList ) != ListType:
      return S_ERROR( "_transaction: wrong type (%s) for cmdList" % type( cmdList ) )

    # # get connection
    connection = conn
    if not connection:
      retDict = self.__getConnection()
      if not retDict['OK']:
        return retDict
      connection = retDict[ 'Value' ]

    # # list with cmds and their results
    cmdRet = []
    try:
      cursor = connection.cursor()
      for cmd in cmdList:
        cmdRet.append( ( cmd, cursor.execute( cmd ) ) )
      connection.commit()
    except Exception, error:
      self.logger.execption( error )
      # # rollback, put back connection to the pool
      connection.rollback()
      return S_ERROR( error )
    # # close cursor, put back connection to the pool
    cursor.close()
    return S_OK( cmdRet )

  def _createViews( self, viewsDict, force = False ):
    """ create view based on query

    :param dict viewDict: { 'ViewName': "Fields" : { "`a`": `tblA.a`, "`sumB`" : "SUM(`tblB.b`)" }
                                        "SelectFrom" : "tblA join tblB on tblA.id = tblB.id",
                                        "Clauses" : [ "`tblA.a` > 10", "`tblB.Status` = 'foo'" ] ## WILL USE AND CLAUSE
                                        "GroupBy": [ "`a`" ],
                                        "OrderBy": [ "`b` DESC" ] }
    """
    if force:
      gLogger.debug( viewsDict )

      for viewName, viewDict in viewsDict.items():

        viewQuery = [ "CREATE OR REPLACE VIEW `%s`.`%s` AS" % ( self.__dbName, viewName ) ]

        columns = ",".join( [ "%s AS %s" % ( colDef, colName )
                             for colName, colDef in  viewDict.get( "Fields", {} ).items() ] )
        tables = viewDict.get( "SelectFrom", "" )
        if columns and tables:
          viewQuery.append( "SELECT %s FROM %s" % ( columns, tables ) )

        where = " AND ".join( viewDict.get( "Clauses", [] ) )
        if where:
          viewQuery.append( "WHERE %s" % where )

        groupBy = ",".join( viewDict.get( "GroupBy", [] ) )
        if groupBy:
          viewQuery.append( "GROUP BY %s" % groupBy )

        orderBy = ",".join( viewDict.get( "OrderBy", [] ) )
        if orderBy:
          viewQuery.append( "ORDER BY %s" % orderBy )

        viewQuery.append( ";" )
        viewQuery = " ".join( viewQuery )
        self.log.debug( "`%s` VIEW QUERY IS: %s" % ( viewName, viewQuery ) )
        createView = self._query( viewQuery )
        if not createView["OK"]:
          gLogger.error( createView["Message"] )
          return createView
    return S_OK()

  def _createTables( self, tableDict, force = False, okIfTableExists = True ):
    """
    tableDict:
      tableName: { 'Fields' : { 'Field': 'Description' },
                   'ForeignKeys': {'Field': 'Table.key' },
                   'PrimaryKey': 'Id',
                   'Indexes': { 'Index': [] },
                   'UniqueIndexes': { 'Index': [] },
                   'Engine': 'InnoDB' }
      only 'Fields' is a mandatory key.

    Creates a new Table for each key in tableDict, "tableName" in the DB with
    the provided description.
    It allows to create:
      - flat tables if no "ForeignKeys" key defined.
      - tables with foreign keys to auxiliary tables holding the values
      of some of the fields
    Arguments:
      tableDict: dictionary of dictionary with description of tables to be created.
      Only "Fields" is a mandatory key in the table description.
        "Fields": Dictionary with Field names and description of the fields
        "ForeignKeys": Dictionary with Field names and name of auxiliary tables.
          The auxiliary tables must be defined in tableDict.
        "PrimaryKey": Name of PRIMARY KEY for the table (if exist).
        "Indexes": Dictionary with definition of indexes, the value for each
          index is the list of fields to be indexed.
        "UniqueIndexes": Dictionary with definition of indexes, the value for each
          index is the list of fields to be indexed. This indexes will declared
          unique.
        "Engine": use the given DB engine, InnoDB is the default if not present.
      force:
        if True, requested tables are DROP if they exist.
        if False (default), tables are not overwritten
      okIfTableExists:
        if True (default), returns S_OK if table exists 
        if False, returns S_ERROR if table exists 
    """

    # First check consistency of request
    if type( tableDict ) != DictType:
      return S_ERROR( 'Argument is not a dictionary: %s( %s )'
                      % ( type( tableDict ), tableDict ) )

    tableList = tableDict.keys()
    if len( tableList ) == 0:
      return S_OK( 0 )
    for table in tableList:
      thisTable = tableDict[table]
      # Check if Table is properly described with a dictionary
      if type( thisTable ) != DictType:
        return S_ERROR( 'Table description is not a dictionary: %s( %s )'
                        % ( type( thisTable ), thisTable ) )
      if not 'Fields' in thisTable:
        return S_ERROR( 'Missing `Fields` key in `%s` table dictionary' % table )

    tableCreationList = [[]]

    auxiliaryTableList = []

    i = 0
    extracted = True
    while tableList and extracted:
      # iterate extracting tables from list if they only depend on
      # already extracted tables.
      extracted = False
      auxiliaryTableList += tableCreationList[i]
      i += 1
      tableCreationList.append( [] )
      for table in list( tableList ):
        toBeExtracted = True
        thisTable = tableDict[table]
        if 'ForeignKeys' in thisTable:
          thisKeys = thisTable['ForeignKeys']
          for key, auxTable in thisKeys.items():
            forTable = auxTable.split( '.' )[0]
            forKey = key
            if forTable != auxTable:
              forKey = auxTable.split( '.' )[1]
            if forTable not in auxiliaryTableList:
              toBeExtracted = False
              break
            if not key in thisTable['Fields']:
              return S_ERROR( 'ForeignKey `%s` -> `%s` not defined in Primary table `%s`.'
                              % ( key, forKey, table ) )
            if not forKey in tableDict[forTable]['Fields']:
              return S_ERROR( 'ForeignKey `%s` -> `%s` not defined in Auxiliary table `%s`.'
                              % ( key, forKey, forTable ) )

        if toBeExtracted:
          self.log.debug( 'Table %s ready to be created' % table )
          extracted = True
          tableList.remove( table )
          tableCreationList[i].append( table )

    if tableList:
      return S_ERROR( 'Recursive Foreign Keys in %s' % ', '.join( tableList ) )

    createdTablesList = []

    for tableList in tableCreationList:
      for table in tableList:
        # Check if Table exists
        retDict = self.__checkTable( table, force = force )
        if not retDict['OK']:
          message = 'The requested table already exists'
          if retDict['Message'] == message and okIfTableExists:
            continue
          return retDict

        thisTable = tableDict[table]
        cmdList = []
        for field in thisTable['Fields'].keys():
          cmdList.append( '`%s` %s' % ( field, thisTable['Fields'][field] ) )

        if thisTable.has_key( 'PrimaryKey' ):
          if type( thisTable['PrimaryKey'] ) in StringTypes:
            cmdList.append( 'PRIMARY KEY ( `%s` )' % thisTable['PrimaryKey'] )
          else:
            cmdList.append( 'PRIMARY KEY ( %s )' % ", ".join( [ "`%s`" % str( f ) for f in thisTable['PrimaryKey'] ] ) )

        if thisTable.has_key( 'Indexes' ):
          indexDict = thisTable['Indexes']
          for index in indexDict:
            indexedFields = '`, `'.join( indexDict[index] )
            cmdList.append( 'INDEX `%s` ( `%s` )' % ( index, indexedFields ) )

        if thisTable.has_key( 'UniqueIndexes' ):
          indexDict = thisTable['UniqueIndexes']
          for index in indexDict:
            indexedFields = '`, `'.join( indexDict[index] )
            cmdList.append( 'UNIQUE INDEX `%s` ( `%s` )' % ( index, indexedFields ) )
        if 'ForeignKeys' in thisTable:
          thisKeys = thisTable['ForeignKeys']
          for key, auxTable in thisKeys.items():

            forTable = auxTable.split( '.' )[0]
            forKey = key
            if forTable != auxTable:
              forKey = auxTable.split( '.' )[1]

            # cmdList.append( '`%s` %s' % ( forTable, tableDict[forTable]['Fields'][forKey] )
            cmdList.append( 'FOREIGN KEY ( `%s` ) REFERENCES `%s` ( `%s` )'
                            ' ON DELETE RESTRICT' % ( key, forTable, forKey ) )

        if thisTable.has_key( 'Engine' ):
          engine = thisTable['Engine']
        else:
          engine = 'InnoDB'

        cmd = 'CREATE TABLE `%s` (\n%s\n) ENGINE=%s' % (
               table, ',\n'.join( cmdList ), engine )
        retDict = self._update( cmd, debug = True )
        if not retDict['OK']:
          return retDict
        self.log.debug( 'Table %s created' % table )
        createdTablesList.append( table )

    return S_OK( createdTablesList )

  def _getFields( self, tableName, outFields = None,
                  inFields = None, inValues = None,
                  limit = False, conn = None,
                  older = None, newer = None,
                  timeStamp = None, orderAttribute = None ):
    """
      Wrapper to the new method for backward compatibility
    """
    self.log.warn( '_getFields:', 'deprecation warning, use getFields methods instead of _getFields.' )
    retDict = _checkFields( inFields, inValues )
    if not retDict['OK']:
      self.log.warn( '_getFields:', retDict['Message'] )
      return retDict

    condDict = {}
    if inFields != None:
      try:
        condDict.update( [ ( inFields[k], inValues[k] ) for k in range( len( inFields ) )] )
      except Exception, x:
        return S_ERROR( x )

    return self.getFields( tableName, outFields, condDict, limit, conn, older, newer, timeStamp, orderAttribute )

  def _insert( self, tableName, inFields = None, inValues = None, conn = None ):
    """
      Wrapper to the new method for backward compatibility
    """
    self.log.warn( '_insert:', 'deprecation warning, use insertFields methods instead of _insert.' )
    return self.insertFields( tableName, inFields, inValues, conn )


  def _to_value( self, param ):
    """
      Convert to string
    """
    return str( param[0] )


  def _to_string( self, param ):
    """
    """
    return param[0].tostring()

  def _getConnection( self ):
    """
    Return a new connection to the DB
    It uses the private method __getConnection
    """
    self.log.debug( '_getConnection:' )

    retDict = self.__getConnection( trial = 0 )
    return retDict

  def __getConnection( self, conn = None, trial = 0 ):
    """
    Return a new connection to the DB,
    if conn is provided then just return it.
    then try the Queue, if it is empty add a newConnection to the Queue and retry
    it will retry MAXCONNECTRETRY to open a new connection and will return
    an error if it fails.
    """
    self.log.debug( '__getConnection:' )

    if not self.__initialized:
      error = 'DB not properly initialized'
      gLogger.error( error )
      return S_ERROR( error )

    return self.__connectionPool.get( self.__dbName )

########################################################################################
#
#  Transaction functions
#
########################################################################################

  def transactionStart( self ):
    return self.__connectionPool.transactionStart( self.__dbName )

  def transactionCommit( self ):
    return self.__connectionPool.transactionCommit( self.__dbName )

  def transactionRollback( self ):
    return self.__connectionPool.transactionRollback( self.__dbName )

  @property
  def transaction( self ):
    """ Transaction guard """
    class TransactionGuard( object ):
      def __init__( self, db ):
        self.__db = db
        self.__ok = False
      def __enter__( self ):
        self.__db.transactionStart()
        def commitWard( *args ):
          self.__ok = True
          return args
        return commitWard
      def __exit__( self, exType, exValue, traceback ):
        if exValue or not self.__ok:
          self.__db.transactionRollback()
        else:
          self.__db.transactionCommit()
    return TransactionGuard( self )




########################################################################################
#
#  Utility functions
#
########################################################################################

  def countEntries( self, table, condDict, older = None, newer = None, timeStamp = None, connection = False,
                    greater = None, smaller = None ):
    """
      Count the number of entries wit the given conditions
    """
    table = _quotedList( [table] )
    if not table:
      error = 'Invalid table argument'
      self.log.debug( 'countEntries:', error )
      return S_ERROR( error )

    try:
      cond = self.buildCondition( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp,
                                  greater = None, smaller = None )
    except Exception, x:
      return S_ERROR( x )

    cmd = 'SELECT COUNT(*) FROM %s %s' % ( table, cond )
    res = self._query( cmd , connection, debug = True )
    if not res['OK']:
      return res

    return S_OK( res['Value'][0][0] )

########################################################################################
  def getCounters( self, table, attrList, condDict, older = None, newer = None, timeStamp = None, connection = False,
                   greater = None, smaller = None ):
    """
      Count the number of records on each distinct combination of AttrList, selected
      with condition defined by condDict and time stamps
    """
    table = _quotedList( [table] )
    if not table:
      error = 'Invalid table argument'
      self.log.debug( 'getCounters:', error )
      return S_ERROR( error )

    attrNames = _quotedList( attrList )
    if attrNames == None:
      error = 'Invalid updateFields argument'
      self.log.debug( 'getCounters:', error )
      return S_ERROR( error )

    try:
      cond = self.buildCondition( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp,
                                  greater = None, smaller = None )
    except Exception, x:
      return S_ERROR( x )

    cmd = 'SELECT %s, COUNT(*) FROM %s %s GROUP BY %s ORDER BY %s' % ( attrNames, table, cond, attrNames, attrNames )
    res = self._query( cmd , connection, debug = True )
    if not res['OK']:
      return res

    resultList = []
    for raw in res['Value']:
      attrDict = {}
      for i in range( len( attrList ) ):
        attrDict[attrList[i]] = raw[i]
      item = ( attrDict, raw[len( attrList )] )
      resultList.append( item )
    return S_OK( resultList )

#########################################################################################
  def getDistinctAttributeValues( self, table, attribute, condDict = None, older = None,
                                  newer = None, timeStamp = None, connection = False,
                                  greater = None, smaller = None ):
    """
      Get distinct values of a table attribute under specified conditions
    """
    table = _quotedList( [table] )
    if not table:
      error = 'Invalid table argument'
      self.log.debug( 'getDistinctAttributeValues:', error )
      return S_ERROR( error )

    attributeName = _quotedList( [attribute] )
    if not attributeName:
      error = 'Invalid attribute argument'
      self.log.debug( 'getDistinctAttributeValues:', error )
      return S_ERROR( error )

    try:
      cond = self.buildCondition( condDict = condDict, older = older, newer = newer, timeStamp = timeStamp,
                                  greater = None, smaller = None )
    except Exception, x:
      return S_ERROR( x )

    cmd = 'SELECT  DISTINCT( %s ) FROM %s %s ORDER BY %s' % ( attributeName, table, cond, attributeName )
    res = self._query( cmd, connection, debug = True )
    if not res['OK']:
      return res
    attr_list = [ x[0] for x in res['Value'] ]
    return S_OK( attr_list )

#############################################################################
  def buildCondition( self, condDict = None, older = None, newer = None,
                      timeStamp = None, orderAttribute = None, limit = False,
                      greater = None, smaller = None, offset = None ):
    """ Build SQL condition statement from provided condDict and other extra check on
        a specified time stamp.
        The conditions dictionary specifies for each attribute one or a List of possible
        values
        greater and smaller are dictionaries in which the keys are the names of the fields,
        that are requested to be >= or < than the corresponding value.
        For compatibility with current usage it uses Exceptions to exit in case of
        invalid arguments
    """
    condition = ''
    conjunction = "WHERE"

    if condDict != None:
      for aName, attrValue in condDict.items():
        if type( aName ) in StringTypes:
          attrName = _quotedList( [aName] )
        elif type( aName ) == TupleType:
          attrName = '('+_quotedList( list( aName ) )+')'
        if not attrName:
          error = 'Invalid condDict argument'
          self.log.warn( 'buildCondition:', error )
          raise Exception( error )
        if type( attrValue ) == ListType:
          retDict = self._escapeValues( attrValue )
          if not retDict['OK']:
            self.log.warn( 'buildCondition:', retDict['Message'] )
            raise Exception( retDict['Message'] )
          else:
            escapeInValues = retDict['Value']
            multiValue = ', '.join( escapeInValues )
            condition = ' %s %s %s IN ( %s )' % ( condition,
                                                    conjunction,
                                                    attrName,
                                                    multiValue )
            conjunction = "AND"

        else:
          retDict = self._escapeValues( [ attrValue ] )
          if not retDict['OK']:
            self.log.warn( 'buildCondition:', retDict['Message'] )
            raise Exception( retDict['Message'] )
          else:
            escapeInValue = retDict['Value'][0]
            condition = ' %s %s %s = %s' % ( condition,
                                               conjunction,
                                               attrName,
                                               escapeInValue )
            conjunction = "AND"

    if timeStamp:
      timeStamp = _quotedList( [timeStamp] )
      if not timeStamp:
        error = 'Invalid timeStamp argument'
        self.log.warn( 'buildCondition:', error )
        raise Exception( error )
      if newer:
        retDict = self._escapeValues( [ newer ] )
        if not retDict['OK']:
          self.log.warn( 'buildCondition:', retDict['Message'] )
          raise Exception( retDict['Message'] )
        else:
          escapeInValue = retDict['Value'][0]
          condition = ' %s %s %s >= %s' % ( condition,
                                              conjunction,
                                              timeStamp,
                                              escapeInValue )
          conjunction = "AND"
      if older:
        retDict = self._escapeValues( [ older ] )
        if not retDict['OK']:
          self.log.warn( 'buildCondition:', retDict['Message'] )
          raise Exception( retDict['Message'] )
        else:
          escapeInValue = retDict['Value'][0]
          condition = ' %s %s %s < %s' % ( condition,
                                             conjunction,
                                             timeStamp,
                                             escapeInValue )

    if type( greater ) == DictType:
      for attrName, attrValue in greater.items():
        attrName = _quotedList( [attrName] )
        if not attrName:
          error = 'Invalid greater argument'
          self.log.warn( 'buildCondition:', error )
          raise Exception( error )

        retDict = self._escapeValues( [ attrValue ] )
        if not retDict['OK']:
          self.log.warn( 'buildCondition:', retDict['Message'] )
          raise Exception( retDict['Message'] )
        else:
          escapeInValue = retDict['Value'][0]
          condition = ' %s %s %s >= %s' % ( condition,
                                             conjunction,
                                             attrName,
                                             escapeInValue )
          conjunction = "AND"

    if type( smaller ) == DictType:
      for attrName, attrValue in smaller.items():
        attrName = _quotedList( [attrName] )
        if not attrName:
          error = 'Invalid smaller argument'
          self.log.warn( 'buildCondition:', error )
          raise Exception( error )

        retDict = self._escapeValues( [ attrValue ] )
        if not retDict['OK']:
          self.log.warn( 'buildCondition:', retDict['Message'] )
          raise Exception( retDict['Message'] )
        else:
          escapeInValue = retDict['Value'][0]
          condition = ' %s %s %s < %s' % ( condition,
                                             conjunction,
                                             attrName,
                                             escapeInValue )
          conjunction = "AND"


    orderList = []
    orderAttrList = orderAttribute
    if type( orderAttrList ) != ListType:
      orderAttrList = [ orderAttribute ]
    for orderAttr in orderAttrList:
      if orderAttr == None:
        continue
      if type( orderAttr ) not in StringTypes:
        error = 'Invalid orderAttribute argument'
        self.log.warn( 'buildCondition:', error )
        raise Exception( error )

      orderField = _quotedList( orderAttr.split( ':' )[:1] )
      if not orderField:
        error = 'Invalid orderAttribute argument'
        self.log.warn( 'buildCondition:', error )
        raise Exception( error )

      if len( orderAttr.split( ':' ) ) == 2:
        orderType = orderAttr.split( ':' )[1].upper()
        if orderType in [ 'ASC', 'DESC']:
          orderList.append( '%s %s' % ( orderField, orderType ) )
        else:
          error = 'Invalid orderAttribute argument'
          self.log.warn( 'buildCondition:', error )
          raise Exception( error )
      else:
        orderList.append( orderAttr )

    if orderList:
      condition = "%s ORDER BY %s" % ( condition, ', '.join( orderList ) )

    if limit:
      if offset:
        condition = "%s LIMIT %d OFFSET %d" % ( condition, limit, offset )
      else:
        condition = "%s LIMIT %d" % ( condition, limit )

    return condition

#############################################################################
  def getFields( self, tableName, outFields = None,
                 condDict = None,
                 limit = False, conn = None,
                 older = None, newer = None,
                 timeStamp = None, orderAttribute = None,
                 greater = None, smaller = None ):
    """
      Select "outFields" from "tableName" with condDict
      N records can match the condition
      return S_OK( tuple(Field,Value) )
      if outFields == None all fields in "tableName" are returned
      if limit is not False, the given limit is set
      inValues are properly escaped using the _escape_string method, they can be single values or lists of values.
    """
    table = _quotedList( [tableName] )
    if not table:
      error = 'Invalid tableName argument'
      self.log.warn( 'getFields:', error )
      return S_ERROR( error )

    quotedOutFields = '*'
    if outFields:
      quotedOutFields = _quotedList( outFields )
      if quotedOutFields == None:
        error = 'Invalid outFields arguments'
        self.log.warn( 'getFields:', error )
        return S_ERROR( error )

    self.log.verbose( 'getFields:', 'selecting fields %s from table %s.' %
                          ( quotedOutFields, table ) )

    if condDict == None:
      condDict = {}

    try:
      try:
        mylimit = limit[0]
        myoffset = limit[1]
      except:
        mylimit = limit
        myoffset = None
      condition = self.buildCondition( condDict = condDict, older = older, newer = newer,
                        timeStamp = timeStamp, orderAttribute = orderAttribute, limit = mylimit,
                        greater = None, smaller = None, offset = myoffset )
    except Exception, x:
      return S_ERROR( x )

    return self._query( 'SELECT %s FROM %s %s' %
                        ( quotedOutFields, table, condition ), conn, debug = True )

#############################################################################
  def deleteEntries( self, tableName,
                     condDict = None,
                     limit = False, conn = None,
                     older = None, newer = None,
                     timeStamp = None, orderAttribute = None,
                     greater = None, smaller = None ):
    """
      Delete rows from "tableName" with
      N records can match the condition
      if limit is not False, the given limit is set
      String type values will be appropriately escaped, they can be single values or lists of values.
    """
    table = _quotedList( [tableName] )
    if not table:
      error = 'Invalid tableName argument'
      self.log.warn( 'deleteEntries:', error )
      return S_ERROR( error )

    self.log.verbose( 'deleteEntries:', 'deleting rows from table %s.' % table )

    try:
      condition = self.buildCondition( condDict = condDict, older = older, newer = newer,
                                       timeStamp = timeStamp, orderAttribute = orderAttribute, limit = limit,
                                       greater = None, smaller = None )
    except Exception, x:
      return S_ERROR( x )

    return self._update( 'DELETE FROM %s %s' % ( table, condition ), conn, debug = True )

#############################################################################
  def updateFields( self, tableName, updateFields = None, updateValues = None,
                    condDict = None,
                    limit = False, conn = None,
                    updateDict = None,
                    older = None, newer = None,
                    timeStamp = None, orderAttribute = None,
                    greater = None, smaller = None ):
    """
      Update "updateFields" from "tableName" with "updateValues".
      updateDict alternative way to provide the updateFields and updateValues
      N records can match the condition
      return S_OK( number of updated rows )
      if limit is not False, the given limit is set
      String type values will be appropriately escaped.

    """
    if not updateFields and not updateDict:
      return S_OK( 0 )

    table = _quotedList( [tableName] )
    if not table:
      error = 'Invalid tableName argument'
      self.log.warn( 'updateFields:', error )
      return S_ERROR( error )

    retDict = _checkFields( updateFields, updateValues )
    if not retDict['OK']:
      error = 'Mismatch between updateFields and updateValues.'
      self.log.warn( 'updateFields:', error )
      return S_ERROR( error )

    if updateFields == None:
      updateFields = []
      updateValues = []

    if updateDict:
      if type( updateDict ) != DictType:
        error = 'updateDict must be a of Type DictType'
        self.log.warn( 'updateFields:', error )
        return S_ERROR( error )
      try:
        updateFields += updateDict.keys()
        updateValues += [updateDict[k] for k in updateDict.keys()]
      except TypeError:
        error = 'updateFields and updateValues must be a list'
        self.log.warn( 'updateFields:', error )
        return S_ERROR( error )

    updateValues = self._escapeValues( updateValues )
    if not updateValues['OK']:
      self.log.warn( 'updateFields:', updateValues['Message'] )
      return updateValues
    updateValues = updateValues['Value']

    self.log.verbose( 'updateFields:', 'updating fields %s from table %s.' %
                          ( ', '.join( updateFields ), table ) )

    try:
      condition = self.buildCondition( condDict = condDict, older = older, newer = newer,
                        timeStamp = timeStamp, orderAttribute = orderAttribute, limit = limit,
                        greater = None, smaller = None )
    except Exception, x:
      return S_ERROR( x )

    updateString = ','.join( ['%s = %s' % ( _quotedList( [updateFields[k]] ),
                                            updateValues[k] ) for k in range( len( updateFields ) ) ] )

    return self._update( 'UPDATE %s SET %s %s' %
                         ( table, updateString, condition ), conn, debug = True )

#############################################################################
  def insertFields( self, tableName, inFields = None, inValues = None, conn = None, inDict = None ):
    """
      Insert a new row in "tableName" assigning the values "inValues" to the
      fields "inFields".
      String type values will be appropriately escaped.
    """
    table = _quotedList( [tableName] )
    if not table:
      error = 'Invalid tableName argument'
      self.log.warn( 'insertFields:', error )
      return S_ERROR( error )

    retDict = _checkFields( inFields, inValues )
    if not retDict['OK']:
      self.log.warn( 'insertFields:', retDict['Message'] )
      return retDict

    if inFields == None:
      inFields = []
      inValues = []

    if inDict:
      if type( inDict ) != DictType:
        error = 'inDict must be a of Type DictType'
        self.log.warn( 'insertFields:', error )
        return S_ERROR( error )
      try:
        inFields += inDict.keys()
        inValues += [inDict[k] for k in inDict.keys()]
      except TypeError:
        error = 'inFields and inValues must be a list'
        self.log.warn( 'insertFields:', error )
        return S_ERROR( error )

    inFieldString = _quotedList( inFields )
    if inFieldString == None:
      error = 'Invalid inFields arguments'
      self.log.warn( 'insertFields:', error )
      return S_ERROR( error )


    inFieldString = '(  %s )' % inFieldString

    retDict = self._escapeValues( inValues )
    if not retDict['OK']:
      self.log.warn( 'insertFields:', retDict['Message'] )
      return retDict
    inValueString = ', '.join( retDict['Value'] )
    inValueString = '(  %s )' % inValueString

    self.log.verbose( 'insertFields:', 'inserting %s into table %s'
                          % ( inFieldString, table ) )

    return self._update( 'INSERT INTO %s %s VALUES %s' %
                         ( table, inFieldString, inValueString ), conn, debug = True )

#####################################################################################
#
#   This is a test code for this class, it requires access to a MySQL DB
#
if __name__ == '__main__':

  import os
  import sys
  from DIRAC.Core.Utilities import Time
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset python optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  gLogger.info( 'Testing MySQL class...' )

  HOST = '127.0.0.1'
  USER = 'Dirac'
  PWD = 'Dirac'
  DB = 'AccountingDB'

  TESTDB = MySQL( HOST, USER, PWD, DB )
  assert TESTDB._connect()['OK']

  TESTDICT = { 'TestTable' : { 'Fields': { 'ID'      : "INTEGER UNIQUE NOT NULL AUTO_INCREMENT",
                                           'Name'    : "VARCHAR(255) NOT NULL DEFAULT 'Yo'",
                                           'Surname' : "VARCHAR(255) NOT NULL DEFAULT 'Tu'",
                                           'Count'   : "INTEGER NOT NULL DEFAULT 0",
                                           'Time'    : "DATETIME",
                                         },
                                'PrimaryKey': 'ID'
                             }
              }

  NAME = 'TestTable'
  FIELDS = [ 'Name', 'Surname' ]
  NEWVALUES = [ 'Name2', 'Surn2' ]
  SOMEFIELDS = [ 'Name', 'Surname', 'Count' ]
  ALLFIELDS = [ 'ID', 'Name', 'Surname', 'Count', 'Time' ]
  ALLVALUES = [ 1, 'Name1', 'Surn1', 1, 'UTC_TIMESTAMP()' ]
  ALLDICT = dict( Name = 'Name1', Surname = 'Surn1', Count = 1, Time = 'UTC_TIMESTAMP()' )
  COND0 = {}
  COND10 = {'Count': range( 10 )}

  try:
    RESULT = TESTDB._createTables( TESTDICT, force = True )
    assert RESULT['OK']
    print 'Table Created'

    RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
    assert RESULT['OK']
    assert RESULT['Value'] == []

    RESULT = TESTDB.getDistinctAttributeValues( NAME, FIELDS[0], COND0 )
    assert RESULT['OK']
    assert RESULT['Value'] == []

    RESULT = TESTDB.getFields( NAME, FIELDS )
    assert RESULT['OK']
    assert RESULT['Value'] == ()

    print 'Inserting'

    for J in range( 100 ):
      RESULT = TESTDB.insertFields( NAME, SOMEFIELDS, ['Name1', 'Surn1', J] )
      assert RESULT['OK']
      assert RESULT['Value'] == 1
      assert RESULT['lastRowId'] == J + 1

    print 'Querying'

    RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
    assert RESULT['OK']
    assert RESULT['Value'] == [( {'Surname': 'Surn1', 'Name': 'Name1'}, 100L )]

    RESULT = TESTDB.getDistinctAttributeValues( NAME, FIELDS[0], COND0 )
    assert RESULT['OK']
    assert RESULT['Value'] == ['Name1']

    RESULT = TESTDB.getFields( NAME, FIELDS )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 100

    RESULT = TESTDB.getFields( NAME, SOMEFIELDS, COND10 )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 10

    RESULT = TESTDB.getFields( NAME, limit = 1 )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 1

    RESULT = TESTDB.getFields( NAME, ['Count'], orderAttribute = 'Count:DESC', limit = 1 )
    assert RESULT['OK']
    assert RESULT['Value'] == ( ( 99, ), )

    RESULT = TESTDB.getFields( NAME, ['Count'], orderAttribute = 'Count:ASC', limit = 1 )
    assert RESULT['OK']
    assert RESULT['Value'] == ( ( 0, ), )

    RESULT = TESTDB.getCounters( NAME, FIELDS, COND10 )
    assert RESULT['OK']
    assert RESULT['Value'] == [( {'Surname': 'Surn1', 'Name': 'Name1'}, 10L )]

    RESULT = TESTDB._getFields( NAME, FIELDS, COND10.keys(), COND10.values() )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 10

    RESULT = TESTDB.updateFields( NAME, FIELDS, NEWVALUES, COND10 )
    assert RESULT['OK']
    assert RESULT['Value'] == 10

    RESULT = TESTDB.updateFields( NAME, FIELDS, NEWVALUES, COND10 )
    assert RESULT['OK']
    assert RESULT['Value'] == 0

    print 'Removing'

    RESULT = TESTDB.deleteEntries( NAME, COND10 )
    assert RESULT['OK']
    assert RESULT['Value'] == 10

    RESULT = TESTDB.deleteEntries( NAME )
    assert RESULT['OK']
    assert RESULT['Value'] == 90

    RESULT = TESTDB.getCounters( NAME, FIELDS, COND0 )
    assert RESULT['OK']
    assert RESULT['Value'] == []

    RESULT = TESTDB.insertFields( NAME, inFields = ALLFIELDS, inValues = ALLVALUES )
    assert RESULT['OK']
    assert RESULT['Value'] == 1

    time.sleep( 1 )

    RESULT = TESTDB.insertFields( NAME, inDict = ALLDICT )
    assert RESULT['OK']
    assert RESULT['Value'] == 1

    time.sleep( 2 )
    RESULT = TESTDB.getFields( NAME, older = 'UTC_TIMESTAMP()', timeStamp = 'Time' )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 2

    RESULT = TESTDB.getFields( NAME, newer = 'UTC_TIMESTAMP()', timeStamp = 'Time' )
    assert len( RESULT['Value'] ) == 0

    RESULT = TESTDB.getFields( NAME, older = Time.toString(), timeStamp = 'Time' )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 2

    RESULT = TESTDB.getFields( NAME, newer = Time.dateTime(), timeStamp = 'Time' )
    assert RESULT['OK']
    assert len( RESULT['Value'] ) == 0

    RESULT = TESTDB.deleteEntries( NAME )
    assert RESULT['OK']
    assert RESULT['Value'] == 2

    print 'OK'

  except AssertionError:
    print 'ERROR ',
    if not RESULT['OK']:
      print RESULT['Message']
    else:
      print RESULT
