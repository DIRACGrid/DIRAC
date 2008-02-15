# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/RemoteBackend.py,v 1.11 2008/02/15 17:45:06 mseco Exp $
__RCSID__ = "$Id: RemoteBackend.py,v 1.11 2008/02/15 17:45:06 mseco Exp $"
"""This Backend sends the Log Messages to a Log Server
It will only report to the server ERROR, EXCEPTION, FATAL
and ALWAYS messages.
"""
import threading
import Queue
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend
from DIRAC.LoggingSystem.private.LogLevels import LogLevels

class RemoteBackend( BaseBackend, threading.Thread ):

  def __init__( self, optionsDictionary ):
    from socket import getfqdn
    from DIRAC.Core.DISET.RPCClient import RPCClient
    threading.Thread.__init__( self )
    self.__interactive = optionsDictionary[ 'Interactive' ]
    self._messageQueue = Queue.Queue()
    self._Transactions = []
    self._alive = True
    self._site = optionsDictionary[ 'Site' ]
    self._domainName = getfqdn()
    self._logLevels = LogLevels()
    self._negativeLevel = self._logLevels.getLevelValue( 'ERROR' ) 
    self._positiveLevel = self._logLevels.getLevelValue( 'ALWAYS' )
    self._maxBundledMessages = 20
    self.oSock = RPCClient( "Logging/SystemLogging", timeout = 10 )
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._messageQueue.put( messageObject )

  def run( self ):
    import time
    while self._alive:
      self._bundleMessages()
      time.sleep(1)

  def _bundleMessages( self ):
    while not self._messageQueue.empty():
      bundle = []
      while ( len( bundle ) < self._maxBundledMessages ) and \
                ( not self._messageQueue.empty() ):
        message = self._messageQueue.get()
        if self._testLevel( message.getLevel() ):
          bundle.append( message.toTuple() )

      if len( bundle ) > 0:
        self._sendMessageToServer( bundle )

  def _sendMessageToServer( self, messageBundle ):
    self._Transactions.append( messageBundle )
    TransactionsLength = len( self._Transactions )
    if TransactionsLength > 100:
      del self._Transactions[:-100]
      TransactionsLength = 100
    while TransactionsLength:
      print self._Transactions[0]
      result = self.oSock.addMessages( self._Transactions[0],
                                       self._site, self._domainName )
      if result['OK']:
        print result['Value']
        TransactionsLength = TransactionsLength - 1
        self._Transactions.pop(0) 
      else:
        print result['Message']
        return False
    return True

  def _testLevel( self, sLevel ):
    messageLevel = self._logLevels.getLevelValue( sLevel )
    return messageLevel <= self._negativeLevel or \
           messageLevel >= self._positiveLevel

  def flush( self ):
    self._alive = False
    if not self._interactive and self._sendMessageToServer():
      while not self._messageQueue.empty():     
        self._bundleMessages()

