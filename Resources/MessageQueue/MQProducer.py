""" MQProducer
"""

from DIRAC import S_ERROR
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService



class MQProducer ( object ):
  def __init__(self, mqManager, mqURI, producerId):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._id = producerId


  def put(self, msg):
    conn =  self._connectionManager.getConnector(getMQService(self._mqURI))
    if conn:
      return conn.put(message = msg, destination = self._destination)
    else:
      return S_ERROR("No connection available")

  def close(self):
    return self._connectionManager.closeConnection(mqURI = self._mqURI, messangerId = self._id, messangerType = "producers")

