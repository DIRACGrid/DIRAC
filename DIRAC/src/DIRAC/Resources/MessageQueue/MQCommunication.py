""" General Message Queue Interface to create Consumers and Producers
"""
from DIRAC import gLogger, S_OK
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConsumer import MQConsumer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.Utilities import getMQParamsFromCS
from DIRAC.Resources.MessageQueue.Utilities import generateDefaultCallback

connectionManager = MQConnectionManager()  # To manage the active MQ connections.


def createConsumer(mqURI, callback=generateDefaultCallback()):
    """
    Function creates MQConsumer. All parameters are taken from the
    Configuration Service based on the mqURI value.

    Args:
      mqURI(str):Pseudo URI identifing MQ service. It has the following format
                mqConnection::DestinationType::DestinationName
                e.g. blabla.cern.ch::Queues::MyQueue1
      callback: callback function that can be used to process the incoming messages

    Returns:
      S_OK/S_ERROR: with the consumer object in S_OK.

    """
    result = _setupConnection(mqURI=mqURI, mType="consumer")
    if not result["OK"]:
        return result
    return S_OK(MQConsumer(mqManager=connectionManager, mqURI=mqURI, consumerId=result["Value"], callback=callback))


def createProducer(mqURI):
    """
    Function creates MQProducer. All parameters are taken from
    the Configuration Service based on the mqURI value.

    Args:
      mqURI(str):Pseudo URI identifing MQ service. It has the following format
                mqConnection::DestinationType::DestinationName
                e.g. blabla.cern.ch::Queues::MyQueue1
    Returns:
      S_OK/S_ERROR: with the producer object in S_OK.
    """
    result = _setupConnection(mqURI=mqURI, mType="producer")
    if not result["OK"]:
        return result
    return S_OK(MQProducer(mqManager=connectionManager, mqURI=mqURI, producerId=result["Value"]))


def _setupConnection(mqURI, mType):
    """Function sets up the active MQ connection. All parameters are taken
        from the Configuration Service based on the mqURI
        value and the messenger Type mType.

    Args:
      mqURI(str):Pseudo URI identifing the MQ service. It has the following format:
                mqConnection::DestinationType::DestinationName
                e.g. blabla.cern.ch::Queues::MyQueue1
      mType(str): 'consumer' or 'producer'
    Returns:
      S_OK/S_ERROR: with the value of the messenger Id ( e.g. 'consumer4' ) in S_OK.
    """
    result = getMQParamsFromCS(mqURI=mqURI)
    if not result["OK"]:
        return result
    params = result["Value"]
    return connectionManager.startConnection(mqURI, params, mType)
