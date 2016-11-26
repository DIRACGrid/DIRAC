""" Class to manage connections for the Message Queue resources.
    Also, set of 'private' helper functions to access and modify the message queue connection storage.
    They are ment to be used only internally by the MQConnectionManager, which should
    assure thread-safe access to it and standard S_OK/S_ERROR error handling.
    MQConnection storage is a dict structure that contains the MQ connections used and reused for
    producer/consumer communication. Example structure:
    {
      mardirac3.in2p3.fr: {'MQConnector':StompConnector, 'destinations':{'/queue/test1':['consumer1', 'producer1'],
                                                                         '/queue/test2':['consumer1', 'producer1']}},
      blabal.cern.ch:     {'MQConnector':None,           'destinations':{'/queue/test2':['consumer2', 'producer2',]}}
    }
"""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress
from DIRAC.Resources.MessageQueue.Utilities import createMQConnector


class MQConnectionManager(object):
  """Manages connections for the Message Queue resources in form of the interal connection storage."""
  def __init__(self, connectionStorage = None):
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self._lock = None
    if connectionStorage:
      self._connectionStorage = connectionStorage
    else:
      self._connectionStorage = {}

  @property
  def lock( self ):
    """ Lock to assure thread-safe access to the internal connection storage.
    """
    if not self._lock:
      self._lock = LockRing().getLock( self.__class__.__name__, recursive = True )
    return self._lock

  def setupConnection(self, mqURI, params, messangerType):
    """ Function add or updates (if already exists) the MQ connection.
    Args:
      mqURI(str):
      params(dict): 
      messangerType(str): 'consumer' or 'producer'.
    Returns:
      S_OK/S_ERROR:
    """
    self.lock.acquire()
    try:
      mqConnectionId = getMQService(mqURI)
      if _connectionExists(self._connectionStorage, mqConnectionId):
        return self.updateConnection(mqURI = mqURI, messangerType = messangerType)
        #_addMessanger(cStorage = self._connectionStorage, mqConnection = connId , destination = destId, messangerId = messangerType) 
      else:
        result = createMQConnector(parameters = params)
        if not result['OK']:
          return result
        connector = result['Value']
        result = connector.setupConnection(parameters = params)
        if not result['OK']:
          return result
        result = connector.connect()
        if not result['OK']:
          return result
        return self.addConnection(mqURI = mqURI, connector = connector, messangerType = messangerType)
    finally:
      self.lock.release()

  def stopConnection(self, mqURI, messangerId, messangerType):
    """Its not really closing connection but more closing it for a given producer
    """
    self.lock.acquire()
    try:
      messangers = self.getMessangersOfAllTypes(getMQService(mqURI), getDestinationAddress(mqURI))
      messangersNew = [m for m in messangers.get(messangerType,[]) if m != messangerId]
      messangers.update({messangerType:messangersNew})
      if not [m for mType in ["consumers", "producers"]  for m in messangers.get(mType, [])]:
        self.getDestinations(mqService = getMQService(mqURI)).pop(getDestinationAddress(mqURI))
      else:
        self.getDestination(mqService = getMQService(mqURI), mqDestinationAddress = getDestinationAddress(mqURI)).update(messangers)
      messangerList = self.getAllMessangersIds(mqService = getMQService(mqURI))
      if not messangerList:
         self.getConnector(getMQService(mqURI)).disconnect()
         self.deleteConnection(getMQService(mqURI))
      return S_OK()
    finally:
      self.lock.release()


  

  def deleteConnection(self, mqService):
    """docstring for deleteConnection"""
    self.lock.acquire()
    try:
      if mqService in self._connectionStorage:
        self._connectionStorage.pop(mqService)
        return S_OK()
      else:
        return S_ERROR()
    finally:
      self.lock.release()


  def updateConnection(self, mqURI, messangerType):
    """docstring for updateConnection"""
    self.lock.acquire()
    try:
      
      messangerList = self.getMessangers(mqService = getMQService(mqURI),
                                        mqDestinationAddress =  getDestinationAddress(mqURI),
                                        messangerType = messangerType)
      messangerId = 1
      if messangerList:
        messangerId =  max(messangerList) + 1
      self.addMessanger(mqService = getMQService(mqURI),
                        mqDestinationAddress =  getDestinationAddress(mqURI),
                        messangerType = messangerType,
                        messangerId = messangerId)
      return S_OK(messangerId)
    finally:
      self.lock.release()

  def addConnection(self, mqURI, connector, messangerType):
    self.lock.acquire()
    try:
      messangers = {"producers":[], "consumers":[]}
      messangers.get(messangerType, []).append(1)
      conn = {"MQConnector":connector, "destinations":{getDestinationAddress(mqURI):messangers}}
      self._connectionStorage.update({getMQService(mqURI):conn})
      return S_OK(1) # it is first messanger so we return his id  = 1
    finally:
      self.lock.release()




  #some helper functions

  def getAllMessangersIds(self, mqService):
    self.lock.acquire()
    try:
      destinationKeys =  self.getDestinations(mqService = mqService).keys()
      if not destinationKeys:
        return []
      return [ mId for dest in destinationKeys for mType in ['consumers', 'producers'] for mId in self.getMessangers(mqService = mqService, mqDestinationAddress = dest, messangerType = mType) ]
    finally:
      self.lock.release()

  def getAllFullMessangersIds(self, mqService):
    messangerList = [ dest + '/'+ mType+ '/'+ str(messanger_id)  for dest in self.getDestinations(mqService = mqService).keys() for mType in ['consumers', 'producers'] for messanger_id in self.getMessangers(mqService = mqService, mqDestinationAddress = dest, messangerType = mType) ]
    print messangerList

  def getConnection(self, mqService):
    """docstring for getConnection"""
    return self._connectionStorage.get(mqService, None)

  def getDestinations(self, mqService):
    """docstring for getDestinations"""
    return self.getConnection(mqService).get("destinations",{})

  def getDestination(self, mqService, mqDestinationAddress):
    return self.getDestinations(mqService = mqService).get(mqDestinationAddress,{})

  def getMessangersOfAllTypes(self, mqService, mqDestinationAddress):
    return self.getDestination(mqService = mqService, mqDestinationAddress = mqDestinationAddress)

  def getMessangers(self, mqService, mqDestinationAddress, messangerType):
    return self.getMessangersOfAllTypes(mqService = mqService, mqDestinationAddress = mqDestinationAddress).get(messangerType,[])

  def addMessanger(self, mqService, mqDestinationAddress, messangerType, messangerId):
    #if queue address key doesnt exist
    if not self.getDestination(mqService = mqService, mqDestinationAddress = mqDestinationAddress):
      self.getDestinations(mqService = mqService)[mqDestinationAddress] = {messangerType:[messangerId]}
    #if messanger type list does not exist
    elif not self.getMessangers(mqService = mqService, mqDestinationAddress =  mqDestinationAddress, messangerType = messangerType):
      self.getDestination(mqService = mqService, mqDestinationAddress = mqDestinationAddress)[messangerType]=[messangerId]
    else:
      self.getMessangers(mqService = mqService, mqDestinationAddress =  mqDestinationAddress, messangerType = messangerType).append(messangerId)

  def connectionExist(self, mqService):
    return mqService in self._connectionStorage

  def getConnector(self, mqService):
    """docstring for getConnector"""
    return self._connectionStorage.get(mqService, {}).get("MQConnector", None)

# Set of 'private' helper functions to access and modify the message queue connection storage.

def _getConnection(cStorage, mqConnection):
  """ Function returns message queue connection from the storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    dict: or None
  """
  return cStorage.get(mqConnection, {})

def _getConnector(cStorage, mqConnection):
  """ Function returns MQConnector from the storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    MQConnector or None
  """
  return _getConnection(cStorage, mqConnection).get("MQConnector", None)

def _getDestinations(cStorage, mqConnection):
  """ Function returns dict with destinations (topics and queues) for given connection.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    dict: of form {'/queue/queue1':['producer1','consumer2']} or {}
  """
  return _getConnection(cStorage, mqConnection).get("destinations", {})

def _getMessangersId(cStorage, mqConnection, mqDestination):
  """ Function returns list of messangers for given connection and given destination.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
  Returns:
    list: of form ['producer1','consumer2'] or []
  """
  return _getDestinations(cStorage, mqConnection).get(mqDestination, [])

def _getMessangersIdWithType(cStorage, mqConnection, mqDestination, messangerType):
  """ Function returns list of messnager for given connection, destination and type.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerType(str): 'consumer' or 'producer'
  Returns:
    list: of form ['producer1','producer2'], ['consumer8', 'consumer20'] or []
  """
  return [p for p in _getMessangersId(cStorage, mqConnection, mqDestination) if messangerType in p]

def _getAllMessangersInfo(cStorage):
  """ Function returns list of all messangers in the pseudo-path format.
  Args:
    cStorage(dict): message queue connection storage.
  Returns:
    list: of form ['blabla.cern.ch/queue/myQueue1/producer1','bibi.in2p3.fr/topic/myTopic331/consumer3'] or []
  """
  output = lambda connection,dest,messanger: str(connection)+str(dest)+'/'+ str(messanger)
  return [output(c, d, m) for c in cStorage.keys() for d in _getDestinations(cStorage,c)  for m in _getMessangersId(cStorage,c, d)]

def _connectionExists(cStorage, mqConnection):
  """ Function checks if given connection exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    bool:
  """
  return mqConnection in cStorage

def _destinationExists(cStorage, mqConnection, mqDestination):
  """ Function checks if given destination(queue or topic) exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
  Returns:
    bool:
  """
  return mqDestination in _getDestinations(cStorage, mqConnection)

def _MessangerExists(cStorage, mqConnection, mqDestination, messangerId):
  """ Function checks if given messanger(producer or consumer) exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4' .
  Returns:
    bool:
  """
  return messangerId in _getMessangersId(cStorage, mqConnection, mqDestination)

def _addMessanger(cStorage, mqConnection, destination, messangerId):
  """ Function adds a messanger(producer or consumer) to given connection and destination.
      If connection or/and destination do not exist, they are created as well.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4'.
  Returns:
    bool: True if messanger is added or False if the messanger already exists.
  """
  if _MessangerExists(cStorage, mqConnection, destination, messangerId):
    return False
  if _connectionExists(cStorage,mqConnection):
    if _destinationExists(cStorage,mqConnection, destination):
      _getMessangersId(cStorage, mqConnection, destination).append(messangerId)
    else:
      _getDestinations(cStorage,mqConnection)[destination] = [messangerId]
  else:
    cStorage[mqConnection]={"MQConnector":None,"destinations":{destination:[messangerId]}}
  return True

def _removeMessanger(cStorage, mqConnection, destination, messangerId):
  """ Function removes  messanger(producer or consumer) from given connection and destination.
      If it is the last messanger in given destination and/or connection they are removed as well..
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4'.
  Returns:
    bool: True if messanger is removed or False if the messanger was not in the storage.
  """
  messangers = _getMessangersId(cStorage, mqConnection, destination)
  destinations = _getDestinations(cStorage,mqConnection)
  if messangerId in messangers:
    messangers.remove(messangerId)
    if not messangers: #If no more messangers we remove the destination.
      destinations.pop(destination)
      if not destinations: #If no more destinations we remove the connection
        cStorage.pop(mqConnection)
    return True
  else:
    return False #messanger was not in the storage
