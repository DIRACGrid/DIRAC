# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ThreadSafe.py,v 1.1 2007/12/19 18:01:49 acasajus Exp $
__RCSID__ = "$Id: ThreadSafe.py,v 1.1 2007/12/19 18:01:49 acasajus Exp $"

import threading

class Synchronizer:
  """ Class enapsulating a lock
  allowing it to be used as a synchronizing
  decorator making the call thread-safe"""

  def __init__( self, lockName = "", recursive = False ):
    self.lockName = lockName
    if recursive:
      self.lock = threading.RLock()
    else:
      self.lock = threading.Lock()

  def __call__( self, funcToCall ):
    def lockedFunc( *args, **kwargs ):
      try:
        if self.lockName:
          print "LOCKING", self.lockName
        self.lock.acquire()
        return funcToCall(*args, **kwargs)
      finally:
        if self.lockName:
          print "UNLOCKING", self.lockName
        self.lock.release()
    return lockedFunc
