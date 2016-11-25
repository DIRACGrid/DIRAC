""" MQConsumer
"""

from DIRAC import S_ERROR
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService

class MQConsumer ( object ):
  def __init__(self, mqManager, mqURI, consumerId, callback = None):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._id = consumerId
    self._callback = callback
#subscribing to connection
    conn =  self._connectionManager.getConnector(getMQService(self._mqURI))
    if conn:
      conn.subscribe(parameters = {'messangerId':self._id, 'callback':callback, 'destination':self._destination})

  def close(self):
    conn =  self._connectionManager.getConnector(getMQService(self._mqURI))
    if conn:
      conn.unsubscribe(parameters = {'destination':self._destination, 'messangerId':self._id})
    return self._connectionManager.closeConnection(mqURI = self._mqURI, messangerId = self._id, messangerType = "consumers")

