"""Class to manage connections for the Message Queue resources."""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress

from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN
from itertools import chain

import collections

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



  def deleteConnection(self, mqService):
    """docstring for deleteConnection"""
    self.lock.acquire()
    try:
      if mqService in self._connectionStorage:
        #if self._connectionStorage[mqService]['MQConnector']:
          #self._connectionStorage[mqService]['MQConnector'].disconnect()
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
      print "ee"
      print messangerType 
      print messangers
      print self._connectionStorage
      print "ee"
      return S_OK(1) # it is first messanger so we return his id  = 1
    finally:
      self.lock.release()


  def closeConnection(self, mqURI, messangerId, messangerType):
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


  def addOrUpdateConnection(self, mqURI, params, messangerType):
    self.lock.acquire()
    try:
      if self.connectionExist(getMQService(mqURI)):
        return self.updateConnection(mqURI = mqURI, messangerType = messangerType)
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
