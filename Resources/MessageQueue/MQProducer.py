""" MQProducer
"""

from DIRAC import S_ERROR
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService

class MQProducer ( object ):
  def __init__(self, mqManager, mqURI, producerId):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._mqService = getMQService(self._mqURI)
    self._id = producerId

  def put(self, msg):
    result =  self._connectionManager.getConnector(self._mqService)
    if result['OK']:
      connector = result['Value']
      return connector.put(message = msg, parameters = {'destination':self._destination})
    return result

  def close(self):
    return self._connectionManager.stopConnection(mqURI = self._mqURI, messangerId = self._id)
