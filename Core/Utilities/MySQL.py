########################################################################
# $Id: MySQL.py,v 1.2 2007/05/15 17:34:24 rgracian Exp $
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

__RCSID__ = "$Id: MySQL.py,v 1.2 2007/05/15 17:34:24 rgracian Exp $"


from DIRAC                                  import gLogger
from DIRAC                                  import S_OK, S_ERROR

import MySQLdb

import Queue
import time
import string
import threading
from types import StringTypes

maxConnectRetry = 10

class MySQL:
  """
  Basic multithreaded DIRAC MySQL Client Class
  """

  def __init__( self, hostName, userName, passwd, dbName, maxQueueSize=10 ):
    """
    set MySQL connection parameters and try to connect
    """
 
    self.__initialized = False
    self._connected = False
    
    try:
      # This allows derived classes from MySQL to define their onw
      # self.logger and will not be overwritten.
      test = self.logger
    except:
      self.logger = gLogger.getSubLogger( 'MySQL' )
    
    # let the derived class decide what to do with if is not 1
    self._threadsafe = MySQLdb.thread_safe()
    self.logger.verbose( 'thread_safe = %s' % self._threadsafe )

    self.__checkQueueSize( maxQueueSize )

    self.__hostName   = str( hostName )
    self.__userName   = str( userName )
    self.__passwd     = str( passwd )
    self.__dbName     = str( dbName )
    # Create the connection Queue to reuse connections
    self.__connectionQueue     = Queue.Queue( maxQueueSize )
    # Create the connection Semaphore to limit total number of open connection
    self.__connectionSemaphore = threading.Semaphore( maxQueueSize )

    self.__initialized = True
    # Atempt an inital connection to the DB, let the derived class decide 
    # what to do if 
    self._connect()
    

  def __del__( self ):

    while 1 and self.__initialized:
      self.__connectionSemaphore.release()
      try:
        connection = self.__connectionQueue.get_nowait()
        connection.close()
      except Queue.Empty,x:
        self.logger.verbose( 'No more connection in Queue' )
        break


  def __checkQueueSize( self, maxQueueSize ):

    if maxQueueSize <= 0:
      raise Exception( 'MySQL.__init__: maxQueueSize must positive' )
    try:
      test = maxQueueSize - 1
    except:
      raise Exception( 'MySQL.__init__: wrong type for maxQueueSize' )


  def _except( self, methodName, v, err ):
    """
    print MySQL error or exeption
    return S_ERROR with Exception
    """

    try:
      raise v
    except MySQLdb.Error,e:
      self.logger.error( '%s: %s' % ( methodName, err ), 
                     '%d: %s' % ( e.args[0], e.args[1] ) )
      return S_ERROR( '%s: ( %d: %s )' % ( err, e.args[0], e.args[1] ) )
    except Exception,x:
      self.logger.error( '%s: %s' % ( methodName, err ), str(x) )
      return S_ERROR( '%s: (%s)' % ( err, str(x) ) )


  def __checkFields( self, inFields, inValues ):

    if len(inFields) != len(inValues):
      return S_ERROR( 'Missmatch between inFields and inValues.' )
    return S_OK()


  def __escapeString( self, s, connection ):
    """ 
    To be used for escaping any MySQL string before passing it to the DB
    this should prevent passing non-MySQL acepted characters to the DB
    It also includes quotation marks " around the given string
    """

    try:
      escape_string = connection.escape_string( str(s) )
      self.logger.debug( '__scape_string: returns', '"%s"' % escape_string )
      return S_OK( '"%s"' % escape_string )
    except Exception,x:
      self.logger.error( '__escape_string: Could not escape string', '"%s"' %s )
      return self._except( '__escape_string',x,'Could not escape string' )


  def _escapeString( self, s, conn = False ):
    self.logger.verbose( '_scapeString:', '"%s"' %s )

    retDict = self.__getConnection( conn )
    if not retDict['OK'] : return retDict
    connection = retDict['Value']

    retDict = self.__escapeString( s, connection )
    if not conn:
      self.__putConnection(connection)

    return retDict

 
  def _escapeValues( self, inValues = [] ):
    """
    Escapes all strings in the list of values provided
    """
    self.logger.verbose( '_escapeValues:', '%s' % inValues )
    
    retDict = self.__getConnection()
    if not retDict['OK'] : return retDict
    connection = retDict['Value']

    inEscapeValues = []

    for value in inValues:
      if type( value ) in StringTypes:
        retDict = self.__escapeString( value, connection )
        if not retDict['OK']:
          self.__putConnection(connection)
          return retDict
        inEscapeValues.append( retDict['Value'] )
      else:
        inEscapeValues.append( str( value )  )
    self.__putConnection(connection)
    return S_OK( inEscapeValues )
  

  def _connect( self ):
    """
    open connection to MySQL DB and put Connection into Queue
    set connected flag to True and return S_OK 
    return S_ERROR upon failure
    """
    self.logger.verbose( '_connect:', self._connected )
    if self._connected:
      return S_OK()

    self.logger.debug( '_connect: Attempting to access DB',
                        '[%s@%s] by user %s/%s.' % 
                        ( self.__dbName, self.__hostName, self.__userName, self.__passwd ) ) 
    try:
      self.__newConnection()
      self.logger.verbose( '_connect: Connected.' )
      self._connected = True
      return S_OK()
    except Exception, x:
      self.logger.showStack()
      return self._except( '_connect', x, 'Could not connect to DB.' )


  def _query( self, cmd, conn = False ):
    """
    execute MySQL query command 
    return S_OK structure with fetchall result as tuple
    it returns an empty tuple if no matching rows are found
    return S_ERROR upon error
    """
    self.logger.verbose( '_query:', cmd)

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK'] : return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      if cursor.execute(cmd):
        res = cursor.fetchall()
      else:
        res = ()
      self.logger.verbose( '_query:', res )
      retDict = S_OK( res )
    except Exception ,x:
      self.logger.error( '_query:', cmd )
      retDict = self._except( '_query', x, 'Excution failed.' )

    try:
      cursor.close()
    except Exception, v:
      pass
    if not conn:
      self.__putConnection(connection)

    return retDict


  def _update(self,cmd, conn=False ):
    """ execute MySQL update command 
        return S_OK with number of updated registers upon success
        return S_ERROR upon error
    """
    self.logger.verbose( '_update:', cmd )

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK'] : return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      res = cursor.execute(cmd)
      connection.commit()
      self.logger.verbose( '_update: %s.' % res )
      retDict =  S_OK(res)
    except Exception,x:
      self.logger.error( '_update: "%s".' % cmd )
      retDict = self._except( '_update', x, 'Execution failed.' )

    try:
      cursor.close()
    except Exception, v:
      pass
    if not conn:
      self.__putConnection(connection)

    return retDict


  def _insert( self, tableName, inFields = [], inValues = [], conn=False ):
    """
    Insert a new row in "tableName" assigning the values "inValues" to the 
    fields "inFields". 
    String type values will be appropiatelly escaped.
    """
    quotedInFields = []
    for field in inFields:
      quotedInFields.append( '`%s`' % field )
    inFieldString = string.join( quotedInFields, ', ' )

    self.logger.verbose( '_insert:', 'inserting ( %s ) into table `%s`' 
                          % ( inFieldString, tableName ) )
                          
    retDict = self.__checkFields( inFields, inValues )
    if not retDict['OK']: return retDict

    retDict = self._escapeValues( inValues )
    if not retDict['OK']: return retDict

    inValueString = string.join( retDict['Value'], ', ' )

    return self._update( 'INSERT INTO `%s` ( %s ) VALUES ( %s )' %
                         ( tableName, inFieldString, inValueString ), conn )


  def _getFields( self, tableName, outFields = [], 
                  inFields = [], inValues = [], 
                  limit = 0, conn=False ):
    """
    Select "outFields" from "tableName" with
    conditions lInFields = lInValues
    N records can match the condition
    return S_OK( tuple(Field,Value) )
    if outFields = [] all fields in "tableName" are returned
    if inFields and inValues are [], no condition is imposed
    if limit is not 0, the given limit is set
    Strings inValues are properly scaped using the _escape_string method.
    """
    self.logger.verbose( '_getFields:', 'selecting fields %s from table `%s`.' %
                          ( str(outFields), tableName ) )

    quotedOutFields = []
    for field in outFields:
      quotedOutFields.append( '`%s`' % field )

    outFieldString = string.join(quotedOutFields,', ')
    if not outFieldString: outFieldString = '*'

    retDict = self.__checkFields( inFields, inValues )
    if not retDict['OK']:
      self.logger.error( '_getFields: %s' % retDict['Message'] )
      return retDict

    retDict = self._escapeValues( inValues )
    if not retDict['OK']: return retDict
    escapeInValues = retDict['Value']    
    
    condition = ''

    for i in range(len(inFields)):
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
    return str(param[0])


  def _to_string( self, param ):
    return param[0].tostring()


  def __newConnection(self):
    """
    Create a New connection and put it in the Queue
    """
    self.logger.debug( '__newConnection:' )

    connection = MySQLdb.connect( host=self.__hostName, 
                                  user=self.__userName, 
                                  passwd=self.__passwd, 
                                  db=self.__dbName)
    self.__putConnection( connection )


  def __putConnection(self,connection):
    """
    Put a connection in the Queue, if the queue is full, the connection is closed
    """
    self.logger.debug( '__putConnection:' )

    # Release the semaphore first, in case something fails
    self.__connectionSemaphore.release()
    try:
      self.__connectionQueue.put_nowait(connection)
    except Queue.Full, x:
      self.logger.debug( '__putConnection: Full Queue' )
      try:
        connection.close()
      except:
        pass
    except Exception, x:
      self._except('__putConnection',x,'Failed to put Connection in Queue')
      
  def _getConnection( self ):
    """
    Return a new connection to the DB
    It uses the private method __getConnection
    """
    self.logger.verbose( '_getConnection:' )

    retDict = self.__getConnection( trial = 0 )
    self.__connectionSemaphore.release()
    return retDict

  def __getConnection( self, conn = False, trial = 0 ):
    """
    Return a new connection to the DB,
    if conn is provided then just return it.
    then try the Queue, if it is empty add a newConnection to the Queue and retry  
    it will retry maxConnectRetry to open a new connection and will return 
    an error if it fails.
    """
    self.logger.debug( '__getConnection:' )

    if conn: return S_OK( conn )
    
    try:
      self.__connectionSemaphore.acquire()
      connection = self.__connectionQueue.get_nowait()
      self.logger.debug( '__getConnection: Got a connection from Queue')
      if connection:
        try:
          # This will try to reconect if the connection has timeout
          connection.ping(True)
        except:
          # if the ping fails try with a new connection from the Queue
          self.__connectionSemaphore.release()
          return self.__getConnection()
        return S_OK(connection)
    except Queue.Empty,x:
      self.__connectionSemaphore.release()
      self.logger.debug( '__getConnection: Empty Queue' )
      try:
        if trial == min(10,maxConnectRetry):
          return S_ERROR( 'Could not get a connection after %s retries.' % maxConnectRetry )
        try:
          self.__newConnection()
          return self.__getConnection( )
        except Exception, x:
          self.logger.error( '__getConnection: Fails to get connection from Queue', x )
          time.sleep( trial * 5.0 )
          newtrial = trial + 1
          return self.__getConnection( trial = newtrial )
      except Exception,x:
        return self._except('__getConnection:',x,'Failed to get connection from Queue')
    except Exception,x:
      return self._except('__getConnection:',x,'Failed to get connection from Queue')


