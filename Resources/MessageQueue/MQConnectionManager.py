"""Class to manage connections for the Message Queue resources."""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing

class MQConnectionManager(object):
  """Manages connections for the Message Queue resources."""
  def __init__(self, connectionStorage = None):
    # We call disconnect() if the connection should be removed.
    #self._connDict = DictCache( deleteFunction = lambda x : x['connection'].disconnect() )
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self._lock = None
    if connectionStorage:
      self._connectionStorage = connectionStorage
    else:
      self._connectionStorage = {}

  @property
  def lock( self ):
    """ lock """
    if not self._lock:
      self._lock = LockRing().getLock( self.__class__.__name__, recursive = True )
    return self._lock


  def connectionExist(self, mqServiceId):
    return mqServiceId in self._connectionStorage

  def getConnection(self, mqServiceId):
    """docstring for getConnection"""
    return self._connectionStorage.get(mqServiceId, None)


  def addConnectionIfNotExist(self, connectionInfo, mqServiceId):
    self.lock.acquire()
    try:
      if not mqServiceId in self._connectionStorage:
        print "add connection"
        self._connectionStorage[mqServiceId] = connectionInfo
      else :
        print "connection already exists"
      return self._connectionStorage[mqServiceId]
    finally:
      self.lock.release()

  def deleteConnection(self, mqServiceId):
    """docstring for deleteConnection"""
    self.lock.acquire()
    try:
      if mqServiceId in self._connectionStorage:
        if self._connectionStorage[mqServiceId]['MQConnection']:
          self._connectionStorage[mqServiceId]['MQConnection'].disconnect()
        self._connectionStorage.pop(mqServiceId)
        return True
      else:
        return False
    finally:
      self.lock.release()

  def updateConnection(self, mqServiceId, destInfoToAdd):
    """docstring for updateConnection"""
    self.lock.acquire()
    try:
      res = self._connectionStorage.get(mqServiceId,  None)
      return res
    finally:
      self.lock.release()
