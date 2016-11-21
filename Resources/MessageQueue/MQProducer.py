""" MQProducer
"""

from DIRAC import S_ERROR
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress, getMQService



class MQProducer ( object ):
  def __init__(self, mqManager, mqURI):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = getDestinationAddress(self._mqURI)
    self._id = 1


  def put(self, msg):
    conn =  self._connectionManager.getConnector(getMQService(self._mqURI))
    if conn:
      return conn.put(message = msg, destination = self._destination)
    else:
      print "some error with connector"
      #to change
      return S_ERROR("")

  def close(self):
    conn =  self._connectionManager.getConnector(getMQService(self._mqURI))
    if conn:
      #we should unsubscribe from connectionManager messangerType and Id?
      return conn.disconnect()
    else:
      print "some error with connector"
      #to change
      return S_ERROR("")

