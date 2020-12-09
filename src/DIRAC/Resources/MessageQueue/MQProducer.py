""" MQProducer
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import gLogger
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService


class MQProducer (object):
  def __init__(self, mqManager, mqURI, producerId):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._mqService = getMQService(self._mqURI)
    self._id = producerId
    self.log = gLogger.getSubLogger(self.__class__.__name__)

  def put(self, msg):
    result = self._connectionManager.getConnector(self._mqService)
    if result['OK']:
      connector = result['Value']
      return connector.put(message=msg, parameters={'destination': self._destination})
    return result

  def close(self):
    """ Function closes the connection for this client.
        The producer id is removed from the connection storage.
        It is not guaranteed that the connection will be
        removed cause other messengers can be still using it.

    Returns:
      S_OK or S_ERROR: Error appears in case if the connection was already
        closed for this producer.
    """
    return self._connectionManager.stopConnection(mqURI=self._mqURI, messengerId=self._id)
