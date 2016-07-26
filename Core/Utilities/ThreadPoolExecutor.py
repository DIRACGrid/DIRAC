"""
ThreadPoolExecutor implements the Pool abstarct interface and is using concurrent.futures.ThreadPoolExecutor.
Note: You can not use the ThreadPoolExecutor with 'with' for example:
   with ThreadPoolExecutor ( 2 ) as executor:
     executor.submit()

"""
import sys

try:
  import concurrent.futures
except ImportError:
  missingLibrary = True
  
try:
  from DIRAC.FrameworkSystem.Client.Logger import gLogger
except ImportError:
  import logging as gLogger  
  #Note: We may want to test the ThreadPoolExecutor without DIRAC installed.
  gLogger.basicConfig()

from DIRAC.Core.Utilities.Pool import Pool
  
class ThreadPoolExecutor ( Pool ):
    
  def __init__( self, maxThreads ):
    super( ThreadPoolExecutor, self ).__init__()
    self.__threadPool = concurrent.futures.ThreadPoolExecutor( maxThreads ) 
            
  def getMaxThreads( self ):
    """
    It returns the maximum threads.
    """
    return self.__threadPool._max_workers
  
  def getMinThreads( self ):
    """
    It returns the minimum threads
    """
    return 0  # Note: In this case it is not used. 
  
  def numWorkingThreads( self ):
    """
    It returns the number of running threads
    """
    return len( self.__threadPool._threads )
  
  def numWaitingThreads( self ):
    """
    It returns the number of waiting threads
    """
    return self.__threadPool._work_queue.qsize()
  
  def generateJobAndQueueIt( self,
                             oCallable,
                             args = tuple(),
                             kwargs = dict(),
                             sTJId = None,
                             oCallback = None,
                             oExceptionCallback = None,
                             blocking = None ):
    """
    :param object oCallable the method which will be executed on a thread.
    :param tuple args the arguments will be used by oCallable
    :param dict kwargs the arguments will be passed to the oCallable. The argument preceded by an identifier (e.g. name=).
    for example: color(code=1, white=False)
    :param int sTJId thread job id
    :param object oCallback it is a callback function. When the result is obtained then the callback function is used.
    :param oExceptionCallback If an exception occur in the target function, then the oExceptionCallback is called with the exception instance.
    :param bool blocking  If block is true, block if necessary until a free slot (thread) is available.
    :return S_OK / S_ERROR
    """
    if sTJId:
      gLogger.warn( "ThreadPoolExecutor does not support sTJId!" )
  
    if blocking:
      gLogger.warn( "blocking is irrelevant for the ThreadPoolExecutor!" )    
    
    try:      
      future = self.__threadPool.submit( oCallable, *args, **kwargs )
      if oCallback:
        future.add_done_callback( oCallback )
    except Exception:
      if oExceptionCallback:
        oExceptionCallback( sys.exc_info() )
    return future
  
if __name__ == "__main__":
  import random
  import time
  
  def testJob( i ):
    print 'Got: %s' % i 
    time.sleep( random.randint( 1, 5 ) )
    return i
  
  executor = ThreadPoolExecutor( 4 ) 
  for i in xrange( 6 ):
    result = executor.generateJobAndQueueIt( testJob, args = ( i, ) )
  
  print 'Waiting threads: %d' % executor.numWaitingThreads()
  print 'Working threads: %d' % executor.numWorkingThreads()
