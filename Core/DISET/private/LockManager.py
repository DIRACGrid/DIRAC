# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/LockManager.py,v 1.1 2007/03/09 15:27:47 rgracian Exp $
__RCSID__ = "$Id: LockManager.py,v 1.1 2007/03/09 15:27:47 rgracian Exp $"

import threading

class LockManager:
  
  def __init__( self, iMaxThreads = None ):
    self.iMaxThreads = iMaxThreads
    if iMaxThreads:
      self.oGlobalLock = threading.Semaphore( iMaxThreads )
    else:
      self.oGlobalLock = False
    self.dLocks = {}
    self.dSubManagers = {}
    
  def createNewLock( self, sLockName, iMaxThreads ):
    if sLockName in self.dLocks.keys():
      raise RuntimeError( "%s lock already exists" % sLockName )
    self.dLocks[ sLockName ] = threading.Semaphore( iMaxThreads )
    self.dLocks[ sLockName ].release()
    
  def lockGlobal( self ):
    if self.oGlobalLock:
      self.oGlobalLock.acquire()
    
  def unlockGlobal( self ):
    if self.oGlobalLock:
      self.oGlobalLock.release()
    
  def lock( self, sLockName ):
    try:
      self.dLocks[ sLockName ].acquire()
    except KeyError:
      raise KeyError( "Lock %s has not been defined" % sLockName )

  def unlock( self, sLockName ):
    try:
      self.dLocks[ sLockName ].release()
    except KeyError:
      raise KeyError( "Lock %s has not been defined" % sLockName )
    
  def createNewLockManager( self, sLockManagerName ):
    if sLockManagerName in self.dSubManagers.keys():
      raise RuntimeError( "%s lock already exists" % sLockName )
    self.dSubManagers[ sLockManagerName ] = LockManager( self.iMaxThreads ) 
    return self.dSubManagers[ sLockManagerName ]
    
  def getLockManager( self, sLockManagerName ):
    try:
      return self.dSubManagers[ sLockManagerName ]
    except KeyError:
      raise KeyError( "Sub LockManager %s has not been defined" % sLockName )        
