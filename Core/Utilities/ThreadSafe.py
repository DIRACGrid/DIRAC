from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"


class Synchronizer(object):
  """ Class encapsulating a lock
  allowing it to be used as a synchronizing
  decorator making the call thread-safe"""

  def __init__(self, lockName="", recursive=False):
    from DIRAC.Core.Utilities.LockRing import LockRing
    self.__lockName = lockName
    self.__lr = LockRing()
    self.__lock = self.__lr.getLock(lockName, recursive=recursive)

  def __call__(self, funcToCall):
    def lockedFunc(*args, **kwargs):
      try:
        if self.__lockName:
          print("LOCKING", self.__lockName)
        self.__lock.acquire()
        return funcToCall(*args, **kwargs)
      finally:
        if self.__lockName:
          print("UNLOCKING", self.__lockName)
        self.__lock.release()
    return lockedFunc

  def lock(self):
    return self.__lock.acquire()

  def unlock(self):
    return self.__lock.release()
