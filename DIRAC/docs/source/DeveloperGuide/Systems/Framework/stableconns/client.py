import sys
import time
from DIRAC import S_ERROR, initialize
from DIRAC.Core.DISET.MessageClient import MessageClient

initialize()


def sendPingMsg(msgClient, pingid=0):
    """
    Send Ping message to the server
    """
    result = msgClient.createMessage("Ping")
    if not result["OK"]:
        return result
    msgObj = result["Value"]
    msgObj.id = pingid
    return msgClient.sendMessage(msgObj)


def pongCB(msgObj):
    """
    Callback for the Pong message.
    Just send a Ping message incrementing in 1 the id
    """
    pongid = msgObj.id
    print("RECEIVED PONG %d" % pongid)
    return sendPingMsg(msgObj.msgClient, pongid + 1)


def disconnectedCB(msgClient):
    """
    Reconnect :)
    """
    retryCount = 0
    while retryCount:
        result = msgClient.connect()
        if result["OK"]:
            return result
        time.sleep(1)
        retryCount -= 1
    return S_ERROR("Could not reconnect... :P")


if __name__ == "__main__":
    msgClient = MessageClient("Framework/PingPong")
    msgClient.subscribeToMessage("Pong", pongCB)
    msgClient.subscribeToDisconnect(disconnectedCB)
    result = msgClient.connect()
    if not result["OK"]:
        print(f"CANNOT CONNECT: {result['Message']}")
        sys.exit(1)
    result = sendPingMsg(msgClient)
    if not result["OK"]:
        print(f"CANNOT SEND PING: {result['Message']}")
        sys.exit(1)
    # Wait 10 secs of pingpongs :P
    time.sleep(10)
