# $HeadURL$
__RCSID__ = "$Id$"

import threading

class Synchronizer:
  """ Class encapsulating a lock
  allowing it to be used as a synchronizing
  decorator making the call thread-safe"""

  def __init__( self, lockName = "", recursive = False ):
    from DIRAC.Core.Utilities.LockRing import LockRing
    self.__lockName = lockName
    self.__lr = LockRing()
    self.__lock = self.__lr.getLock( lockName, recursive = recursive )

  def __call__( self, funcToCall ):
    def lockedFunc( *args, **kwargs ):
      try:
        if self.__lockName:
          print "LOCKING", self.__lockName
        self.__lock.acquire()
        return funcToCall(*args, **kwargs)
      finally:
        if self.__lockName:
          print "UNLOCKING", self.__lockName
        self.__lock.release()
    return lockedFunc

  def lock(self):
    return self.__lock.acquire()

  def unlock(self):
    return self.__lock.release()


#FIXME: not used
class WORM:
  """
  Write One - Read Many
  """
  def __init__( self, maxReads = 10 ):
    from DIRAC.Core.Utilities.LockRing import LockRing
    self.__lr = LockRing()
    self.__lock = self.__lr.getLock()
    self.__maxReads = maxReads
    self.__semaphore = threading.Semaphore( maxReads )

  def write( self, funcToCall ):
    """
    Write decorator
    """
    def __doWriteLock( *args, **kwargs ):
      try:
        self.__startWriteZone()
        return funcToCall(*args, **kwargs)
      finally:
        self.__endWriteZone()
    return __doWriteLock

  def read( self, funcToCall ):
    """
    Read decorator
    """
    def __doReadLock( *args, **kwargs ):
      try:
        self.__startReadZone()
        return funcToCall(*args, **kwargs)
      finally:
        self.__endReadZone()
    return __doReadLock

  def __startWriteZone(self):
    """
    Locks Event to prevent further threads from reading.
    Stops current thread until no other thread is accessing.
    PRIVATE USE
    """
    self.__lock.acquire()
    for i in range( self.__maxReads ):
      self.__semaphore.acquire()
    self.__lock.release()

  def __endWriteZone(self):
    """
    Unlocks Event.
    PRIVATE USE
    """
    for i in range( self.__maxReads ):
      self.__semaphore.release()

  def __startReadZone(self):
    """
    Start of danger zone. This danger zone may be or may not be a mutual exclusion zone.
    Counter is maintained to know how many threads are inside and be able to enable and disable mutual exclusion.
    PRIVATE USE
    """
    self.__semaphore.acquire()

  def __endReadZone( self ):
    """
    End of danger zone.
    PRIVATE USE
    """
    self.__semaphore.release()
