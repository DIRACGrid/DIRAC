""" MQConsumer
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves import queue as Queue
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService, generateDefaultCallback
from DIRAC.Core.Utilities.DErrno import EMQNOM


class MQConsumer (object):
  def __init__(self, mqManager, mqURI, consumerId, callback=generateDefaultCallback()):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._id = consumerId
    self._callback = callback
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    # subscribing to connection
    result = self._connectionManager.getConnector(getMQService(self._mqURI))
    if result['OK']:
      connector = result['Value']
      if connector:
        result = connector.subscribe(
            parameters={
                'messengerId': self._id,
                'callback': callback,
                'destination': self._destination})
        if not result['OK']:
          self.log.error('Failed to subscribe the consumer:' + self._id)
      else:
        self.log.error('Failed to initialize MQConsumer! No MQConnector!')
    else:
      self.log.error('Failed to get MQConnector!')

  def get(self):
    """ Function gets the message
        using the default callback machinery.
        This function can be called only if the the default
        callback function was used !!!!

    Returns:
      S_OK or S_ERROR: Error in case if there are no messages in the
        queue or other error appeared.
        S_OK with the message content otherwise.
    """
    if not self._callback:
      return S_ERROR('No callback set!')
    try:
      msg = self._callback.get()
    except Queue.Empty:
      return S_ERROR(EMQNOM, 'No messages in queue')
    except Exception as e:
      return S_ERROR('Exception: %s' % e)
    else:
      return S_OK(msg)

  def close(self):
    """ Function closes the connection for this client.
        The producer id is removed from the connection storage.
        It is not guaranteed that the connection will be
        removed cause other messengers can be still using it.

    Returns:
      S_OK or S_ERROR: Error appears in case if the connection was already
        closed for this consumer.
    """
    return self._connectionManager.stopConnection(mqURI=self._mqURI, messengerId=self._id)
