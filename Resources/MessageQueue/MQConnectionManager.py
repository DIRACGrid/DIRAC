"""Class to manage connections for the Message Queue resources."""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress

from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN

import collections
def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

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

  def updateConnection(self, mqURI, messangerType):
    """docstring for updateConnection"""
    self.lock.acquire()
    try:
      messangerList = self.getConnection(getMQService(mqURI)).get("destinations",{}).get(getDestinationAddress(mqURI),{}).get(messangerType,[])
      messangerId = 1
      if messangerList:
        messangerId =  max(messangerList) + 1
      self.getConnection(getMQService(mqURI)).get("destinations",{}).get(getDestinationAddress(mqURI),{}).get(messangerType,[]).append(messangerId)
      return messangerId
    finally:
      self.lock.release()

  def addConnection(self, mqURI, connector, messangerType):
    self.lock.acquire()
    try:
      messangers = {"producers":[], "consumers":[]}
      messangers.get(messangerType, []).append(1)
      conn = {"MQConnector":connector, "destinations":{getDestinationAddress(mqURI):messangers}}
      self._connectionStorage.update({getMQService(mqURI):conn})
      return 1 # it is first messanger so we return his id  = 1
    finally:
      self.lock.release()


  def addOrUpdateConnection(self, mqURI, params, messangerType):
    self.lock.acquire()
    try:
      if self.connectionExist(getMQService(mqURI)):
        return self.updateConnection(mqURI = mqURI, messangerType = messangerType)
      else:
        connector = createMQConnector(parameters = params)
        return self.addConnection(mqURI = mqURI, connector = connector, messangerType = messangerType)
    finally:
      self.lock.release()
