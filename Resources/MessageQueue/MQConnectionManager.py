"""Class to manage connections for the Message Queue resources."""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService

from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN


def getSpecializedMQConnector(mqType):
  subClassName = mqType + 'MQConnector'
  objectLoader = ObjectLoader.ObjectLoader()
  result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
  if not result['OK']:
    gLogger.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
  return result

def createMQConnector(parameters = None):
  mqType = parameters.get('MQType', None)
  result = getSpecializedMQConnector(mqType = mqType)
  if not result['OK']:
    gLogger.error( 'Failed to getSpecializedMQConnector:', '%s' % (result['Message'] ) )
    return result
  ceClass = result['Value']
  try:
    mqConnector = ceClass(parameters)
    if not result['OK']:
      return result
  except Exception as exc:
    gLogger.exception( 'Could not instantiate MQConnector object',  lExcInfo = exc )
    return S_ERROR( EMQUKN, '' )
  return S_OK( mqConnector )

class MQConnectionManager(object):
  """Manages connections for the Message Queue resources."""
  def __init__(self, connectionStorage = None):
    # We call disconnect() if the connection should be removed.
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


  def connectionExist(self, mqService):
    return mqService in self._connectionStorage

  def getConnection(self, mqService):
    """docstring for getConnection"""
    return self._connectionStorage.get(mqService, None)

  def getConnector(self, mqService):
    """docstring for getConnector"""
    self.lock.acquire()
    try:
      return self._connectionStorage.get(mqService, {}).get("MQConnector", None)
    finally:
      self.lock.release()

  def addConnectionIfNotExist(self, connectionInfo, mqService):
    self.lock.acquire()
    try:
      if not mqService in self._connectionStorage:
        print "add connection"
        self._connectionStorage[mqService] = connectionInfo
      else :
        print "connection already exists"
      return self._connectionStorage[mqService]
    finally:
      self.lock.release()

  def deleteConnection(self, mqServiceId):
    """docstring for deleteConnection"""
    self.lock.acquire()
    try:
      if mqServiceId in self._connectionStorage:
        if self._connectionStorage[mqServiceId]['MQConnector']:
          self._connectionStorage[mqServiceId]['MQConnector'].disconnect()
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

  def addConnection(self, mqURI, connector, messangerType):
    return 1

  def addOrUpdateConnection(self, mqURI, params, messangerType):
    self.lock.acquire()
    try:
      if self.connectionExist(getMQService(mqURI)):
        return self.updateConnection(mqURI, messangerType)
      else:
        connector = createMQConnector(parameters = params)
        messangerId = self.addConnection(mqURI, connector, messangerType)
        #connector.start()
        return messangerId
    finally:
      self.lock.release()
