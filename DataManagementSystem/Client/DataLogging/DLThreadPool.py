'''
Created on May 4, 2015

@author: Corentin Berger
'''

from threading  import Lock


from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
class DLThreadPool :
  """
    contains all DLSequence needed by different thread
    this class help to have one sequence by thread
    pool is a dictionary with key thread id and with value a DLSequence
  """
  pool = dict()
  # lock for multi-threading
  lock = Lock()


  def __init__( self ):
    pass


  @classmethod
  def getDataLoggingSequence( cls, threadID ):
    """
      return the DLSequence associated to the threadID

      :param threadID: id of the thread

      :return res, S_OK( sequence ) or S_ERROR('Error message')
    """
    cls.lock.acquire()
    if threadID not in cls.pool:
      seq = DLSequence()
      cls.pool[threadID] = seq
    res = cls.pool[threadID]
    cls.lock.release()
    return res

  @classmethod
  def popDataLoggingSequence( cls, threadID ):
    """
      pop an element from the dict and return the value associated to key threadID

      :param threadID: id of the thread
    """
    cls.lock.acquire()
    res = cls.pool.pop( threadID )
    cls.lock.release()
    return res
