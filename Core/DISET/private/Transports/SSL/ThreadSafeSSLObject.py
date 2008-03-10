# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Transports/SSL/ThreadSafeSSLObject.py,v 1.3 2008/03/10 13:42:38 acasajus Exp $
__RCSID__ = "$Id: ThreadSafeSSLObject.py,v 1.3 2008/03/10 13:42:38 acasajus Exp $"

import GSI
import threading
from DIRAC.LoggingSystem.Client.Logger import gLogger

class ThreadSafeSSLObject:
  cLock = threading.RLock()
  def __init__( self, object ):
    self.cObject = object
  def __getattr__( self, name ):
    method = getattr( self.cObject, name )
    if callable( method ):
      return _MagicMethod( self.cLock, method, name )
    else:
      return method

#######################################################################
#
#  Thread-safe hack in order to use multi-threaded applications
#
#######################################################################

class _MagicMethod:
  # some magic to bind a method to an object
  # supports "nested" methods (e.g. examples.getStateName)
  def __init__(self, cLock, method, name ):
    self.sFunctionName = name
    self.cMethod = method
    self.cLock = cLock
    self.iLockDebug = 0
    self.iDebug = 0
  def lock( self ):
    if self.iLockDebug:
      self.cLock.acquire()
    else:
      self.cLock.acquire()
  def unlock( self ):
    if self.iLockDebug:
      self.cLock.release()
    else:
      self.cLock.release()
  def __call__(self, *args):
    self.lock()
    try:
      try:
        returnValue = apply( self.cMethod , args )
      except GSI.SSL.ZeroReturnError:
        returnValue = 0
      except Exception, v:
        if v[0] == -1:
          return 0
        else:
          gLogger.error( "ERROR while executing = %s( %s ) (%s)" % ( self.sFunctionName, str( args )[1:-2], str( v ) ) )
          raise v
    finally:
      self.unlock()
    return returnValue

