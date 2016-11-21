""" MQProducer
"""

from DIRAC import S_OK, S_ERROR

class MQProducer ( object ):
  def __init__(self, mqManager, mqURI):
    self._connectionManager = mqManager
    self._mqURI = mqURI
    self._destination = self._getDestination(self._mqURI)
    self._id = 1
  def _getMQService(self, mqURI):
    return mqURI.split("::")[0]
  def _getDestinationType(self, mqURI):
    return mqURI.split("::")[1]
  def _getDestinationName(self, mqURI):
    return mqURI.split("::")[2]
  def _getDestination(self, mqURI):
    mqType, mqName = mqURI.split("::")[-2:]
    return "/" + mqType.lower() + "/" + mqName


  def put(self, msg):
    conn =  self._connectionManager.getConnector(self._getMQService(self._mqURI))
    if conn:
      return conn.put(message = msg, destination = self._destination)
    else:
      print "some error with connector"
      #to change
      return S_ERROR("")

  def close(self):
    conn =  self._connectionManager.getConnector(self._getMQService(self._mqURI))
    if conn:
      #we should unsubscribe from connectionManager messangerType and Id?
      return conn.disconnect()
    else:
      print "some error with connector"
      #to change
      return S_ERROR("")

