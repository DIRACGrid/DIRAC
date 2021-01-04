""" Class to manage connections for the Message Queue resources.
    Also, set of 'private' helper functions to access and modify the message queue connection storage.
    They are ment to be used only internally by the MQConnectionManager, which should
    assure thread-safe access to it and standard S_OK/S_ERROR error handling.
    MQConnection storage is a dict structure that contains the MQ connections used and reused for
    producer/consumer communication. Example structure::

      {
        mardirac3.in2p3.fr: {'MQConnector':StompConnector, 'destinations':{'/queue/test1':['consumer1', 'producer1'],
                                                                           '/queue/test2':['consumer1', 'producer1']}},
        blabal.cern.ch:     {'MQConnector':None,           'destinations':{'/queue/test2':['consumer2', 'producer2',]}}
      }
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress
from DIRAC.Resources.MessageQueue.MQConnector import createMQConnector
from DIRAC.Core.Utilities.DErrno import EMQCONN


class MQConnectionManager(object):
  """Manages connections for the Message Queue resources in form of the interal connection storage."""

  def __init__(self, connectionStorage=None):
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.__lock = None
    if connectionStorage:
      self.__connectionStorage = connectionStorage
    else:
      self.__connectionStorage = {}

  @property
  def lock(self):
    """ Lock to assure thread-safe access to the internal connection storage.
    """
    if not self.__lock:
      self.__lock = LockRing().getLock(self.__class__.__name__, recursive=True)
    return self.__lock

  def startConnection(self, mqURI, params, messengerType):
    """ Function adds or updates the MQ connection. If the connection
        does not exists, MQconnector is created and added.

    Args:
      mqURI(str):
      params(dict): parameters to initialize the MQConnector.
      messengerType(str): 'consumer' or 'producer'.
    Returns:
      S_OK/S_ERROR: with the value of the messenger Id in S_OK.
    """
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      if self.__connectionExists(conn):
        return self.addNewMessenger(mqURI=mqURI, messengerType=messengerType)
      else:  # Connection does not exist so we create the connector and we add a new connection
        result = self.addNewMessenger(mqURI=mqURI, messengerType=messengerType)
        if not result['OK']:
          return result
        mId = result['Value']
        result = self.createConnectorAndConnect(parameters=params)
        if not result['OK']:
          return result
        if self.__getConnector(conn):
          return S_ERROR(EMQCONN, "The connector already exists!")
        self.__setConnector(conn, result['Value'])
        return S_OK(mId)
    finally:
      self.lock.release()

  def addNewMessenger(self, mqURI, messengerType):
    """ Function updates the MQ connection by adding the messenger Id to the internal connection storage.
        Also the messengerId is chosen.
        messenger Id is set to the maximum existing value (or 0 no messengers are connected) + 1.
        messenger Id is calculated separately for consumers and producers

    Args:
      mqURI(str):
      messengerType(str): 'consumer' or 'producer'.
    Returns:
      S_OK: with the value of the messenger Id or S_ERROR if the messenger was not added,
            cause the same id already exists.
    """
    # 'consumer1' ->1
    # 'producer21' ->21
    def msgIdToInt(msgIds, msgType):
      return [int(m.replace(msgType, '')) for m in msgIds]
    # The messengerId is str e.g.  'consumer5' or 'producer3'

    def generateMessengerId(msgT):
      return msgT + str(max(msgIdToInt(self.__getAllMessengersIdWithType(msgT), msgT) or [0]) + 1)
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      dest = getDestinationAddress(mqURI)
      mId = generateMessengerId(messengerType)
      if self.__addMessenger(conn, dest, mId):
        return S_OK(mId)
      return S_ERROR(EMQCONN, "Failed to update the connection: the messenger %s  already exists" % mId)
    finally:
      self.lock.release()

  def createConnectorAndConnect(self, parameters):
    result = createMQConnector(parameters=parameters)
    if not result['OK']:
      return result
    connector = result['Value']
    result = connector.setupConnection(parameters=parameters)
    if not result['OK']:
      return result
    result = connector.connect()
    if not result['OK']:
      return result
    return S_OK(connector)

  def disconnect(self, connector):
    if not connector:
      return S_ERROR(EMQCONN, 'Failed to disconnect! Connector is None!')
    return connector.disconnect()

  def unsubscribe(self, connector, destination, messengerId):
    if not connector:
      return S_ERROR(EMQCONN, 'Failed to unsubscribe! Connector is None!')
    return connector.unsubscribe(parameters={'destination': destination, 'messengerId': messengerId})

  def getConnector(self, mqConnection):
    """ Function returns MQConnector assigned to the mqURI.

    Args:
      mqConnection(str): connection name.
    Returns:
      S_OK/S_ERROR: with the value of the MQConnector in S_OK if not None
    """
    self.lock.acquire()
    try:
      connector = self.__getConnector(mqConnection)
      if not connector:
        return S_ERROR('Failed to get the MQConnector!')
      return S_OK(connector)
    finally:
      self.lock.release()

  def stopConnection(self, mqURI, messengerId):
    """ Function 'stops' the connection for given messenger, which means
        it removes it from the messenger list. If this is the consumer, the
        unsubscribe() connector method is called. If it is the last messenger
        of this destination (queue or topic), then the destination is removed.
        If it is the last destination from this connection. The disconnect function
        is called and the connection is removed.

    Args:
      mqURI(str):
      messengerId(str): e.g. 'consumer1' or 'producer10'.
    Returns:
      S_OK: with the value of the messenger Id or S_ERROR if the messenger was not added,
            cause the same id already exists.
    """
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      dest = getDestinationAddress(mqURI)
      connector = self.__getConnector(conn)

      if not self.__removeMessenger(conn, dest, messengerId):
        return S_ERROR(EMQCONN, 'Failed to stop the connection!The messenger %s does not exist!' % messengerId)
      else:
        if 'consumer' in messengerId:
          result = self.unsubscribe(connector, destination=dest, messengerId=messengerId)
          if not result['OK']:
            return result
      if not self.__connectionExists(conn):
        return self.disconnect(connector)
      return S_OK()
    finally:
      self.lock.release()

  def getAllMessengers(self):
    """ Function returns a list of all messengers registered in connection storage.

    Returns:
      S_OK or S_ERROR: with the list of strings in the pseudo-path format e.g.
            ['blabla.cern.ch/queue/test1/consumer1','blabal.cern.ch/topic/test2/producer2']
    """
    self.lock.acquire()
    try:
      return S_OK(self.__getAllMessengersInfo())
    finally:
      self.lock.release()

  def removeAllConnections(self):
    """ Function removes all existing connections and calls the disconnect
        for connectors.

    Returns:
      S_OK or S_ERROR:
    """
    self.lock.acquire()
    try:
      connections = self.__getAllConnections()
      for conn in connections:
        connector = self.__getConnector(conn)
        if connector:
          self.disconnect(connector)
      self.__connectionStorage = {}
      return S_OK()
    finally:
      self.lock.release()

  # Set of 'private' helper functions to access and modify the message queue connection storage.
  def __getConnection(self, mqConnection):
    """ Function returns message queue connection from the storage.
    Args:
      mqConnection(str): message queue connection name.
    Returns:
      dict:
    """
    return self.__connectionStorage.get(mqConnection, {})

  def __getAllConnections(self):
    """ Function returns a list of all connection names in the storage
    Returns:
      list:
    """
    return list(self.__connectionStorage)

  def __getConnector(self, mqConnection):
    """ Function returns MQConnector from the storage.
    Args:
      mqConnection(str): message queue connection name.
    Returns:
      MQConnector or None
    """
    return self.__getConnection(mqConnection).get("MQConnector", None)

  def __setConnector(self, mqConnection, connector):
    """ Function returns MQConnector from the storage.
    Args:
      mqConnection(str): message queue connection name.
      connector(MQConnector):
    Returns:
      bool: False if connection does not exit
    """
    connDict = self.__getConnection(mqConnection)
    if not connDict:
      return False
    connDict["MQConnector"] = connector
    return True

  def __getDestinations(self, mqConnection):
    """ Function returns dict with destinations (topics and queues) for given connection.
    Args:
      mqConnection(str): message queue connection name.
    Returns:
      dict: of form {'/queue/queue1':['producer1','consumer2']} or {}
    """
    return self.__getConnection(mqConnection).get("destinations", {})

  def __getMessengersId(self, mqConnection, mqDestination):
    """ Function returns list of messengers for given connection and given destination.
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    Returns:
      list: of form ['producer1','consumer2'] or []
    """
    return self.__getDestinations(mqConnection).get(mqDestination, [])

  def __getMessengersIdWithType(self, mqConnection, mqDestination, messengerType):
    """ Function returns list of messnager for given connection, destination and type.
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
      messengerType(str): 'consumer' or 'producer'
    Returns:
      list: of form ['producer1','producer2'], ['consumer8', 'consumer20'] or []
    """
    return [p for p in self.__getMessengersId(mqConnection, mqDestination) if messengerType in p]

  def __getAllMessengersId(self):
    """ Function returns list of all messengers ids.
        The list can contain duplicates because the same
        producer id can be used for different queues.
    Args:
    Returns:
      list: of form ['producer1','consumer1', 'producer1'] or []
    """
    return [m for c in self.__connectionStorage.keys() for d in self.__getDestinations(c)
            for m in self.__getMessengersId(c, d)]

  def __getAllMessengersIdWithType(self, messengerType):
    """ Function returns list of all messengers ids for given messengerType
    Args:
      messengerType(str): 'consumer' or 'producer'
    Returns:
      list: of form ['producer1','producer2'], ['consumer8', 'consumer20'] or []
    """
    return [p for p in self.__getAllMessengersId() if messengerType in p]

  def __getAllMessengersInfo(self):
    """ Function returns list of all messengers in the pseudo-path format.
    Returns:
      list: of form ['blabla.cern.ch/queue/myQueue1/producer1','bibi.in2p3.fr/topic/myTopic331/consumer3'] or []
    """
    def output(connection, dest, messenger):
      return str(connection) + str(dest) + '/' + str(messenger)
    return [output(c, d, m) for c in self.__connectionStorage.keys()
            for d in self.__getDestinations(c) for m in self.__getMessengersId(c, d)]

  def __connectionExists(self, mqConnection):
    """ Function checks if given connection exists in the connection storage.
    Args:
      mqConnection(str): message queue connection name.
    Returns:
      bool:
    """
    return mqConnection in self.__connectionStorage

  def __destinationExists(self, mqConnection, mqDestination):
    """ Function checks if given destination(queue or topic) exists in the connection storage.
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    Returns:
      bool:
    """
    return mqDestination in self.__getDestinations(mqConnection)

  def __messengerExists(self, mqConnection, mqDestination, messengerId):
    """ Function checks if given messenger(producer or consumer) exists in the connection storage.
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
      messengerId(str): messenger name e.g. 'consumer1', 'producer4' .
    Returns:
      bool:
    """
    return messengerId in self.__getMessengersId(mqConnection, mqDestination)

  def __addMessenger(self, mqConnection, destination, messengerId):
    """ Function adds a messenger(producer or consumer) to given connection and destination.
        If connection or/and destination do not exist, they are created as well.
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
      messengerId(str): messenger name e.g. 'consumer1', 'producer4'.
    Returns:
      bool: True if messenger is added or False if the messenger already exists.
    """
    if self.__messengerExists(mqConnection, destination, messengerId):
      return False
    if self.__connectionExists(mqConnection):
      if self.__destinationExists(mqConnection, destination):
        self.__getMessengersId(mqConnection, destination).append(messengerId)
      else:
        self.__getDestinations(mqConnection)[destination] = [messengerId]
    else:
      self.__connectionStorage[mqConnection] = {"MQConnector": None, "destinations": {destination: [messengerId]}}
    return True

  def __removeMessenger(self, mqConnection, destination, messengerId):
    """ Function removes  messenger(producer or consumer) from given connection and destination.
        If it is the last messenger in given destination and/or connection they are removed as well..
    Args:
      mqConnection(str): message queue connection name.
      mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
      messengerId(str): messenger name e.g. 'consumer1', 'producer4'.
    Returns:
      bool: True if messenger is removed or False if the messenger was not in the storage.
    """
    messengers = self.__getMessengersId(mqConnection, destination)
    destinations = self.__getDestinations(mqConnection)
    if messengerId in messengers:
      messengers.remove(messengerId)
      if not messengers:  # If no more messengers we remove the destination.
        destinations.pop(destination)
        if not destinations:  # If no more destinations we remove the connection
          self.__connectionStorage.pop(mqConnection)
      return True
    else:
      return False  # messenger was not in the storage
