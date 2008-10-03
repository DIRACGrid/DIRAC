
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

class SecurityLogClient:

  __securityLogStore = []

  def __init__(self):
    self.__messagesList = []
    self.__maxMessagesInBundle = 100
    self.__maxMessagesWaiting = 10000
    self.__taskId = gThreadScheduler.addPeriodicTask( 300, self.__sendData )

  def addMessage( self, success, sourceIP, sourcePort, sourceIdentity,
                  destinationIP, destinationPort, destinationService,
                  action, timestamp = False ):
    if not timestamp:
      timestamp = Time.dateTime()
    msg = ( timestamp, success, sourceIP, sourcePort, sourceIdentity,
            destinationIP, destinationPort, destinationService, action )
    while len( self.__messagesList ) > self.__maxMessagesWaiting:
      self.__messagesList.pop(0)
    if not self.__securityLogStore:
      self.__messagesList.append( msg )
    else:
      self.__securityLogStore[0].logAction( msg )

  def setLogStore( self, logStore ):
    while self.__securityLogStore:
      self.__securityLogStore.pop()
    self.__securityLogStore.append( logStore )
    gThreadScheduler.addPeriodicTask( 10, self.__sendData, executions = 1 )

  def __sendData(self):
    msgList = self.__messagesList
    self.__messagesList = []
    rpcClient = RPCClient( "Framework/SecurityLog" )
    for i in range( 0, len( msgList ), self.__maxMessagesInBundle ):
      msgsToSend = msgList[ :self.__maxMessagesInBundle ]
      result = rpcClient.logActionBundle( msgsToSend )
      if not result[ 'OK' ]:
        self.__messagesList.extend( msgList )
        break
      msgList = msgList[ self.__maxMessagesInBundle: ]


