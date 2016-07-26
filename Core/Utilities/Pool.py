"""
An abstract class that provides methods to execute calls asynchronously in a certain Pool (ProcessPool or ThreadPool). 
It should not be used directly, you should use in a subclass.
"""

from abc import ABCMeta, abstractmethod

__RCSID__ = "$Id$"

class Pool( object ): #TODO: python3 class Pool(metaclass=ABCMeta):
  __metaclass__ = ABCMeta
  
  @abstractmethod
  def getMaxThreads( self ):
    """
    It returns the maximum threads.
    """
    pass
  
  @abstractmethod
  def getMinThreads( self ):
    """
    It returns the minimum threads
    """
    pass
  
  @abstractmethod
  def numWorkingThreads( self ):
    """
    It returns the number of running threads
    """
    pass
  
  @abstractmethod
  def numWaitingThreads( self ):
    """
    It returns the number of waiting threads
    """
    pass
  
  @abstractmethod
  def generateJobAndQueueIt( self,
                             oCallable,
                             args = tuple(),
                             kwargs = dict(),
                             sTJId = None,
                             oCallback = None,
                             oExceptionCallback = None,
                             blocking = True ):
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
    pass
