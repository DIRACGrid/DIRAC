""" Utilities for the MessageQueue package
"""
import queue
from DIRAC import S_OK, S_ERROR, gConfig


def getMQParamsFromCS(mqURI):
    """Function gets parameters of a MQ destination (queue/topic) from the CS.

    Args:
      mqURI(str):Pseudo URI identifing the MQ service. It has the following format:
                mqConnection::DestinationType::DestinationName
                e.g. blabla.cern.ch::Queues::MyQueue1
      mType(str): 'consumer' or 'producer'
    Returns:
      S_OK(param_dicts) or S_ERROR
    """
    try:
        mqService, mqType, mqName = mqURI.split("::")
    except ValueError:
        return S_ERROR(f"Bad format of mqURI address:{mqURI}")

    result = gConfig.getConfigurationTree("/Resources/MQServices", mqService, mqType, mqName)
    if not result["OK"] or not result["Value"]:
        return S_ERROR(f"Requested destination not found in the CS: {mqService}::{mqType}::{mqName}")
    mqDestinationPath = None
    for path, value in result["Value"].items():
        if not value and path.endswith(mqName):
            mqDestinationPath = path

    # set-up internal parameter depending on the destination type
    tmp = mqDestinationPath.split("Queues")[0].split("Topics")
    servicePath = tmp[0]
    serviceDict = {}
    if len(tmp) > 1:
        serviceDict["Topic"] = mqName
    else:
        serviceDict["Queue"] = mqName

    result = gConfig.getOptionsDict(servicePath)
    if not result["OK"]:
        return result
    serviceDict.update(result["Value"])

    result = gConfig.getOptionsDict(mqDestinationPath)
    if not result["OK"]:
        return result
    serviceDict.update(result["Value"])
    return S_OK(serviceDict)


def getMQService(mqURI):
    return mqURI.split("::")[0]


def getDestinationType(mqURI):
    return mqURI.split("::")[1]


def getDestinationName(mqURI):
    return mqURI.split("::")[2]


def getDestinationAddress(mqURI):
    mqType, mqName = mqURI.split("::")[-2:]
    # We remove the trailing 's to change from Queues to Queue
    mqType = mqType.rstrip("s")
    return "/" + mqType.lower() + "/" + mqName


def generateDefaultCallback():
    """Function generates a default callback that can
        be used to handle the messages in the MQConsumer
        clients. It contains the internal queue (as closure)
        for the incoming messages. The queue can be accessed by the
        callback.get() method. The callback.get() method returns
        the first message or raise the exception Queue.Empty.
        e.g. myCallback = generateDefaultCallback()

            try:
               print myCallback.get()
            except Queue.Empty:
              pass

    Args:
      mqURI(str):Pseudo URI identifing MQ connection. It has the following format
                mqConnection::DestinationType::DestinationName
                e.g. blabla.cern.ch::Queues::MyQueue1
    Returns:
      object: callback function
    """
    msgQueue = queue.Queue()

    def callback(headers, body):
        msgQueue.put(body)
        return S_OK()

    def get():
        return msgQueue.get(block=False)

    callback.get = get
    return callback
