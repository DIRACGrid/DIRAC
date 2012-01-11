########################################################################
# $HeadURL$
########################################################################
""" DIRAC Basic Oracle Class
    It provides access to the basic Oracle methods in a multithread-safe mode
    keeping used connections in a python Queue for further reuse.

    These are the coded methods:

    __init__( user, passwd, tns, [maxConnsInQueue=10] )

    Initializes the Queue and tries to connect to the DB server,
    using the _connect method.
    "maxConnsInQueue" defines the size of the Queue of open connections
    that are kept for reuse. It also defined the maximum number of open
    connections available from the object.
    maxConnsInQueue = 0 means unlimited and it is not supported.


    _except( methodName, exception, errorMessage )

    Helper method for exceptions: the "methodName" and the "errorMessage"
    are printed with ERROR level, then the "exception" is printed (with
    full description if it is a Oracle Exception) and S_ERROR is returned
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


    _getConnection()

    Gets a connection from the Queue (or open a new one if none is available)
    Returns S_OK with connection in Value or S_ERROR
    the calling method is responsible for closing this connection once it is no
    longer needed.

"""

__RCSID__ = "$Id$"


from DIRAC                                  import gLogger
from DIRAC                                  import S_OK, S_ERROR

import cx_Oracle
import types

gInstancesCount = 0

import Queue
import time
import threading

maxConnectRetry = 100
maxArraysize = 5000 #max allowed
class OracleDB:
  """
  Basic multithreaded DIRAC Oracle Client Class
  """

  def __init__( self, userName, password = '', tnsEntry = '', maxQueueSize = 100 ):
    """
    set Oracle connection parameters and try to connect
    """
    global gInstancesCount
    gInstancesCount += 1

    self.__initialized = False
    self._connected = False

    if 'logger' not in dir( self ):
      self.logger = gLogger.getSubLogger( 'Oracle' )

    # let the derived class decide what to do with if is not 1
    self._threadsafe = cx_Oracle.threadsafety
    self.logger.debug( 'thread_safe = %s' % self._threadsafe )

    self.__checkQueueSize( maxQueueSize )

    self.__userName = str( userName )
    self.__passwd = str( password )
    self.__tnsName = str( tnsEntry )
    # Create the connection Queue to reuse connections
    self.__connectionQueue = Queue.Queue( maxQueueSize )
    # Create the connection Semaphore to limit total number of open connection
    self.__connectionSemaphore = threading.Semaphore( maxQueueSize )

    self.__initialized = True
    self._connect()


  def __del__( self ):
    global gInstancesCount

    while 1 and self.__initialized:
      self.__connectionSemaphore.release()
      try:
        connection = self.__connectionQueue.get_nowait()
        connection.close()
      except Queue.Empty:
        self.logger.debug( 'No more connection in Queue' )
        break

  def __checkQueueSize( self, maxQueueSize ):

    if maxQueueSize <= 0:
      raise Exception( 'OracleDB.__init__: maxQueueSize must positive' )
    try:
      test = maxQueueSize - 1
    except:
      raise Exception( 'OracleDB.__init__: wrong type for maxQueueSize' )


  def _except( self, methodName, x, err ):
    """
    print Oracle error or exeption
    return S_ERROR with Exception
    """

    try:
      raise x
    except cx_Oracle.Error, e:
      self.logger.debug( '%s: %s' % ( methodName, err ),
                     '%s' % ( e ) )
      return S_ERROR( '%s: ( %s )' % ( err, e ) )
    except Exception, x:
      self.logger.debug( '%s: %s' % ( methodName, err ), str( x ) )
      return S_ERROR( '%s: (%s)' % ( err, str( x ) ) )


  def __checkFields( self, inFields, inValues ):

    if len( inFields ) != len( inValues ):
      return S_ERROR( 'Missmatch between inFields and inValues.' )
    return S_OK()


  def _connect( self ):
    """
    open connection to Oracle DB and put Connection into Queue
    set connected flag to True and return S_OK
    return S_ERROR upon failure
    """
    self.logger.debug( '_connect:', self._connected )
    if self._connected:
      return S_OK()

    self.logger.debug( '_connect: Attempting to access DB',
                        'by user %s/%s.' %
                        ( self.__userName, self.__passwd ) )
    try:
      self.__newConnection()
      self.logger.debug( '_connect: Connected.' )
      self._connected = True
      return S_OK()
    except Exception, x:
      return self._except( '_connect', x, 'Could not connect to DB.' )


  def _query( self, cmd, conn = False ):
    """
    execute Oracle query command
    return S_OK structure with fetchall result as tuple
    it returns an empty tuple if no matching rows are found
    return S_ERROR upon error
    """
    self.logger.debug( '_query:', cmd )

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK'] :
      return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      cursor.arraysize = maxArraysize
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
      self.logger.debug( 'Start Roolback transaktio!' )
      connection.rollback()
      self.logger.debug( 'End Roolback transaktio!' )

    try:
      connection.commit()
      cursor.close()
    except Exception:
      pass
    if not conn:
      self.__putConnection( connection )

    return retDict

  def executeStoredProcedure( self, packageName, parameters, output = True, array = None, conn = False ):

    self.logger.debug( '_query:', packageName + "(" + str( parameters ) + ")" )

    retDict = self.__getConnection( conn = conn )
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']

    try:
      cursor = connection.cursor()
      result = None
      results = None
      if array != None and len(array) > 0:
        if type(array[0]) == types.StringType:
          result = cursor.arrayvar( cx_Oracle.STRING, array )
          parameters += [result]
        elif type(array[0]) == types.LongType or type(array[0]) == types.IntType:
          result = cursor.arrayvar( cx_Oracle.NUMBER, array )
          parameters += [result]
        else:
          return S_ERROR('The array type is not supported!!!')
      if output == True:
        result = connection.cursor()
        result.arraysize = maxArraysize # 500x faster!!
        parameters += [result]
        cursor.callproc( packageName, parameters )
        results = result.fetchall()
      else:
        cursor.callproc( packageName, parameters )
      retDict = S_OK( results )
    except Exception , x:

      self.logger.debug( '_query:', packageName + "(" + str( parameters ) + ")" )
      retDict = self._except( '_query', x, 'Execution failed.' )
      connection.rollback()



    try:
      cursor.close()
    except Exception:
      pass
    if not conn:
      self.__putConnection( connection )

    return retDict


  def executeStoredFunctions( self, packageName, returnType, parameters = None, conn = False ):
    if parameters == None:
      parameters = []
    retDict = self.__getConnection( conn = conn )
    if not retDict['OK']:
      return retDict
    connection = retDict['Value']
    try:
      cursor = connection.cursor()
      cursor.arraysize = maxArraysize
      result = cursor.callfunc( packageName, returnType, parameters )
      retDict = S_OK( result )
    except Exception , x:
      self.logger.debug( '_query:', packageName + "(" + str( parameters ) + ")" )
      retDict = self._except( '_query', x, 'Excution failed.' )
      connection.rollback()


    try:
      cursor.close()
    except Exception:
      pass
    if not conn:
      self.__putConnection( connection )
    return retDict

  def __newConnection( self ):
    """
    Create a New connection and put it in the Queue
    """
    self.logger.debug( '__newConnection:' )

    connection = cx_Oracle.Connection( self.__userName, self.__passwd, self.__tnsName, threaded = True )
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
      except Exception:
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

  def __getConnection( self, conn = False, trial = 0 ):
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
          connection.commit()
        except:
          # if the ping fails try with a new connection from the Queue
          self.__connectionSemaphore.release()
          return self.__getConnection()
        return S_OK( connection )
    except Queue.Empty, x:
      self.__connectionSemaphore.release()
      self.logger.debug( '__getConnection: Empty Queue' )
      try:
        if trial == min( 100, maxConnectRetry ):
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


