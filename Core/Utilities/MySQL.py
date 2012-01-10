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

    Attemps connection to DB and sets the _connected flag to True upon success.
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


    _getFields(outFields, tableName, inFields = [], inValues = [] )

    Select "outFields" from "tableName" with conditions lInFields = lInValues
    More than 1 record can match the condition return S_OK( tuple(Field,Value) )
    String type values in inValues are properly escaped.

    _insert( tableName, inFields = [], inValues = [] )

    Insert a new row in "tableName" using the given Fields and Values
    String type values in inValues are properly escaped.

    _getConnection()

    Gets a connection from the Queue (or open a new one if none is available)
    Returns S_OK with connection in Value or S_ERROR
    the calling method is responsible for closing this connection once it is no
    longer needed.

"""

__RCSID__ = "$Id$"


from DIRAC                                  import gLogger
from DIRAC                                  import S_OK, S_ERROR

import MySQLdb
# This is for proper initialization of embeded server, it should only be called once
MySQLdb.server_init( ['--defaults-file=/opt/dirac/etc/my.cnf', '--datadir=/opt/mysql/db'], ['mysqld'] )
gInstancesCount = 0

import Queue
import types
import time
import threading
from types import StringTypes, DictType

maxConnectRetry = 10

def _checkQueueSize( maxQueueSize ):

  if maxQueueSize <= 0:
    raise Exception( 'MySQL.__init__: maxQueueSize must positive' )
  try:
    maxQueueSize - 1
  except Exception:
    raise Exception( 'MySQL.__init__: wrong type for maxQueueSize' )

def _checkFields( inFields, inValues ):

  if inFields == None and inValues == None:
    return S_OK()

  if len( inFields ) != len( inValues ):
    return S_ERROR( 'Missmatch between inFields and inValues.' )
  return S_OK()




class MySQL:
  """
  Basic multithreaded DIRAC MySQL Client Class
  """
  __initialized = False

  def __init__( self, hostName, userName, passwd, dbName, port = 3306, maxQueueSize = 3):
    """
    set MySQL connection parameters and try to connect
    """
    global gInstancesCount
    gInstancesCount += 1

    self._connected = False

    if 'logger' not in dir( self ):
      self.logger = gLogger.getSubLogger( 'MySQL' )

    # let the derived class decide what to do with if is not 1
    self._threadsafe = MySQLdb.thread_safe()
    self.logger.debug( 'thread_safe = %s' % self._threadsafe )

    _checkQueueSize( maxQueueSize )

    self.__hostName = str( hostName )
    self.__userName = str( userName )
    self.__passwd = str( passwd )
    self.__dbName = str( dbName )
    self.__port = port
    # Create the connection Queue to reuse connections
    self.__connectionQueue = Queue.Queue( maxQueueSize )
    # Create the connection Semaphore to limit total number of open connection
    self.__connectionSemaphore = threading.Semaphore( maxQueueSize )

    self.__initialized = True
    self._connect()


  def __del__( self ):
    global gInstancesCount
    try:
      while 1 and self.__initialized:
        self.__connectionSemaphore.release()
        try:
          connection = self.__connectionQueue.get_nowait()
          connection.close()
        except Queue.Empty:
          self.logger.debug( 'No more connection in Queue' )
          break
      if gInstancesCount == 1:
        # only when the last instance of a MySQL object is deleted, the server
        # can be ended
        MySQLdb.server_end()
      gInstancesCount -= 1
    except Exception:
      pass

  def _except( self, methodName, x, err ):
    """
    print MySQL error or exeption
    return S_ERROR with Exception
    """

    try:
      raise x
    except MySQLdb.Error, e:
      self.logger.debug( '%s: %s' % ( methodName, err ),
                     '%d: %s' % ( e.args[0], e.args[1] ) )
      return S_ERROR( '%s: ( %d: %s )' % ( err, e.args[0], e.args[1] ) )
    except Exception, e:
      self.logger.debug( '%s: %s' % ( methodName, err ), str( e ) )
      return S_ERROR( '%s: (%s)' % ( err, str( e ) ) )


  def __escapeString( self, myString, connection ):
    """
    To be used for escaping any MySQL string before passing it to the DB
    this should prevent passing non-MySQL acepted characters to the DB
    It also includes quotation marks " around the given string
    """

    try:
      escape_string = connection.escape_string( str( myString ) )
      self.logger.debug( '__scape_string: returns', '"%s"' % escape_string )
      return S_OK( '"%s"' % escape_string )
    except Exception, x:
      self.logger.debug( '__escape_string: Could not escape string', '"%s"' % myString )
      return self._except( '__escape_string', x, 'Could not escape string' )

  def __checkTable( self, tableName, force = False ):

    cmd = 'SHOW TABLES'
    retDict = self._query( cmd )
    if not retDict['OK']:
      return retDict
    if ( tableName, ) in retDict['Value']:
      if not force:
        # the requested exist and table creation is not force, return with error
        return S_ERROR( 'The requested table already exist' )
      else:
        cmd = 'DROP TABLE `%s`' % tableName
        retDict = self._update( cmd )
        if not retDict['OK']:
          return retDict

    return S_OK()


  def _escapeString( self, myString, conn = None ):
    self.logger.debug( '_scapeString:', '"%s"' % myString )

    retDict = self.__getConnection( conn )
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    retDict = self.__escapeString( myString, connection )
    if not conn:
      self.__putConnection( connection )

    return retDict


  def _escapeValues( self, inValues = None ):
    """
    Escapes all strings in the list of values provided
    """
    self.logger.debug( '_escapeValues:', inValues )

    retDict = self.__getConnection()
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    inEscapeValues = []

    if not inValues:
      return S_OK( inEscapeValues )

    for value in inValues:
      if type( value ) in StringTypes:
        retDict = self.__escapeString( value, connection )
        if not retDict['OK']:
          self.__putConnection( connection )
          return retDict
        inEscapeValues.append( retDict['Value'] )
      else:
        inEscapeValues.append( str( value ) )
    self.__putConnection( connection )
    return S_OK( inEscapeValues )


  def _connect( self ):
    """
    open connection to MySQL DB and put Connection into Queue
    set connected flag to True and return S_OK
    return S_ERROR upon failure
    """
    self.logger.debug( '_connect:', self._connected )
    if self._connected:
      return S_OK()

    self.logger.debug( '_connect: Attempting to access DB',
                        '[%s@%s] by user %s/%s.' %
                        ( self.__dbName, self.__hostName, self.__userName, self.__passwd ) )
    try:
      self.__newConnection()
      self.logger.debug( '_connect: Connected.' )
      self._connected = True
      return S_OK()
    except Exception, x:
      return self._except( '_connect', x, 'Could not connect to DB.' )


  def _query( self, cmd, conn = None ):
    """
    execute MySQL query command
    return S_OK structure with fetchall result as tuple
    it returns an empty tuple if no matching rows are found
    return S_ERROR upon error
    """
    self.logger.debug( '_query:', cmd )

    if conn:
      connection = conn
    else:
      retDict = self._getConnection()
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
      if len( res ) < 10:
        self.logger.debug( '_query:', res )
      else:
        self.logger.debug( '_query: Total %d records returned' % len( res ) )
        self.logger.debug( '_query: %s ...' % str( res[:10] ) )

      retDict = S_OK( res )
    except Exception , x:
      self.logger.debug( '_query:', cmd )
      retDict = self._except( '_query', x, 'Execution failed.' )

    try:
      cursor.close()
    except Exception:
      pass

    return retDict


  def _update( self, cmd, conn = None ):
    """ execute MySQL update command
        return S_OK with number of updated registers upon success
        return S_ERROR upon error
    """
    self.logger.debug( '_update:', cmd )

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      res = cursor.execute( cmd )
      connection.commit()
      self.logger.debug( '_update: %s.' % res )
      retDict = S_OK( res )
      if cursor.lastrowid:
        retDict[ 'lastRowId' ] = cursor.lastrowid
    except Exception, x:
      self.logger.debug( '_update: "%s".' % cmd )
      retDict = self._except( '_update', x, 'Execution failed.' )

    try:
      cursor.close()
    except Exception:
      pass
    if not conn:
      self.__putConnection( connection )

    return retDict


  def _createTables( self, tableDict, force = False ):
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
        if False, returned with S_ERROR if table exist.

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
          self.logger.info( 'Table %s ready to be created' % table )
          extracted = True
          tableList.remove( table )
          tableCreationList[i].append( table )

    if tableList:
      return S_ERROR( 'Recursive Foreign Keys in %s' % ', '.join( tableList ) )

    for tableList in tableCreationList:
      for table in tableList:
        # Check if Table exist
        retDict = self.__checkTable( table, force = force )
        if not retDict['OK']:
          return retDict

        thisTable = tableDict[table]
        cmdList = []
        for field in thisTable['Fields'].keys():
          cmdList.append( '`%s` %s' % ( field, thisTable['Fields'][field] ) )

        if thisTable.has_key( 'PrimaryKey' ):
          if type( thisTable['PrimaryKey'] ) == types.StringType:
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
        retDict = self._update( cmd )
        if not retDict['OK']:
          return retDict
        self.logger.info( 'Table %s created' % table )

    return S_OK()

  def _insert( self, tableName, inFields = None, inValues = None, conn = None ):
    """
    Insert a new row in "tableName" assigning the values "inValues" to the
    fields "inFields".
    String type values will be appropriately escaped.
    """
    quotedInFields = []
    if inFields != None:
      for field in inFields:
        quotedInFields.append( '`%s`' % field )
    inFieldString = ', '.join( quotedInFields )

    self.logger.debug( '_insert:', 'inserting ( %s ) into table `%s`'
                          % ( inFieldString, tableName ) )

    retDict = _checkFields( inFields, inValues )
    if not retDict['OK']:
      return retDict

    retDict = self._escapeValues( inValues )
    if not retDict['OK']:
      return retDict

    inValueString = ', '.join( retDict['Value'] )

    return self._update( 'INSERT INTO `%s` ( %s ) VALUES ( %s )' %
                         ( tableName, inFieldString, inValueString ), conn )


  def _getFields( self, tableName, outFields = None,
                  inFields = None, inValues = None,
                  limit = 0, conn = None ):
    """
    Select "outFields" from "tableName" with
    conditions lInFields = lInValues
    N records can match the condition
    return S_OK( tuple(Field,Value) )
    if outFields == None all fields in "tableName" are returned
    if inFields and inValues are None, no condition is imposed
    if limit is not 0, the given limit is set
    Strings inValues are properly escaped using the _escape_string method.
    """
    self.logger.debug( '_getFields:', 'selecting fields %s from table `%s`.' %
                          ( str( outFields ), tableName ) )

    quotedOutFields = []
    if outFields != None:
      for field in outFields:
        quotedOutFields.append( '`%s`' % field )

    outFieldString = ', '.join( quotedOutFields )
    if not outFieldString:
      outFieldString = '*'

    retDict = _checkFields( inFields, inValues )
    if not retDict['OK']:
      self.logger.debug( '_getFields: %s' % retDict['Message'] )
      return retDict

    retDict = self._escapeValues( inValues )
    if not retDict['OK']:
      return retDict
    escapeInValues = retDict['Value']

    condition = ''

    for i in range( len( inFields ) ):
      field = inFields[i]
      value = escapeInValues[i]
      if condition:
        condition += ' AND'
      condition += ' `%s`=%s' % ( field, value )

    if condition:
      condition = 'WHERE %s ' % condition

    if limit:
      condition += 'LIMIT %s' % limit

    return self._query( 'SELECT %s FROM `%s` %s' %
                        ( outFieldString, tableName, condition ), conn )


  def _to_value( self, param ):
    return str( param[0] )


  def _to_string( self, param ):
    return param[0].tostring()


  def __newConnection( self ):
    """
    Create a New connection and put it in the Queue
    """
    self.logger.debug( '__newConnection:' )

    connection = MySQLdb.connect( host = self.__hostName,
                                  port = self.__port,
                                  user = self.__userName,
                                  passwd = self.__passwd,
                                  db = self.__dbName )
    self.__putConnection( connection )


  def __putConnection( self, connection ):
    """
    Put a connection in the Queue, if the queue is full, the connection is closed
    """
    self.logger.debug( '__putConnection:' )

    # Release the semaphore first, in case something fails
    self.__connectionSemaphore.release()
    try:
      self.__connectionQueue.put_nowait( connection )
    except Queue.Full, x:
      self.logger.debug( '__putConnection: Full Queue' )
      try:
        connection.close()
      except:
        pass
    except Exception, x:
      self._except( '__putConnection', x, 'Failed to put Connection in Queue' )

  def _getConnection( self ):
    """
    Return a new connection to the DB
    It uses the private method __getConnection
    """
    self.logger.debug( '_getConnection:' )

    retDict = self.__getConnection( trial = 0 )
    self.__connectionSemaphore.release()
    return retDict

  def __getConnection( self, conn = None, trial = 0 ):
    """
    Return a new connection to the DB,
    if conn is provided then just return it.
    then try the Queue, if it is empty add a newConnection to the Queue and retry
    it will retry maxConnectRetry to open a new connection and will return
    an error if it fails.
    """
    self.logger.debug( '__getConnection:' )

    if conn:
      return S_OK( conn )

    try:
      self.__connectionSemaphore.acquire()
      connection = self.__connectionQueue.get_nowait()
      self.logger.debug( '__getConnection: Got a connection from Queue' )
      if connection:
        try:
          # This will try to reconect if the connection has timeout
          connection.ping( True )
        except:
          # if the ping fails try with a new connection from the Queue
          self.__connectionSemaphore.release()
          return self.__getConnection()
        return S_OK( connection )
    except Queue.Empty, x:
      self.__connectionSemaphore.release()
      self.logger.debug( '__getConnection: Empty Queue' )
      try:
        if trial == min( 10, maxConnectRetry ):
          return S_ERROR( 'Could not get a connection after %s retries.' % maxConnectRetry )
        try:
          self.__newConnection()
          return self.__getConnection()
        except Exception, x:
          self.logger.debug( '__getConnection: Fails to get connection from Queue', x )
          time.sleep( trial * 5.0 )
          newtrial = trial + 1
          return self.__getConnection( trial = newtrial )
      except Exception, x:
        return self._except( '__getConnection:', x, 'Failed to get connection from Queue' )
    except Exception, x:
      return self._except( '__getConnection:', x, 'Failed to get connection from Queue' )


