# $HeadURL$
__RCSID__ = "$Id$"
"""This Backend sends the Log Messages to a Log Server
It will only report to the server ERROR, EXCEPTION, FATAL
and ALWAYS messages.
"""
import threading
import Queue
from DIRAC.Core.Utilities import Time, Network
from DIRAC.FrameworkSystem.private.logging.backends.BaseBackend import BaseBackend
from DIRAC.FrameworkSystem.private.logging.LogLevels import LogLevels

class RemoteBackend( BaseBackend, threading.Thread ):

  def __init__( self, optionsDictionary ):
    BaseBackend.__init__(self, optionsDictionary)
    threading.Thread.__init__( self )
    self.__interactive = optionsDictionary[ 'Interactive' ]
    self.__sleep = optionsDictionary[ 'SleepTime' ]
    self._messageQueue = Queue.Queue()
    self._Transactions = []
    self._alive = True
    self._site = optionsDictionary[ 'Site' ]
    self._hostname = Network.getFQDN()
    self._logLevels = LogLevels()
    self._negativeLevel = self._logLevels.getLevelValue( 'ERROR' )
    self._positiveLevel = self._logLevels.getLevelValue( 'ALWAYS' )
    self._maxBundledMessages = 20
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._messageQueue.put( messageObject )

  def run( self ):
    import time
    while self._alive:
      self._bundleMessages()
      time.sleep( self.__sleep )

  def _bundleMessages( self ):
    while not self._messageQueue.empty():
      bundle = []
      while ( len( bundle ) < self._maxBundledMessages ) and \
                ( not self._messageQueue.empty() ):
        message = self._messageQueue.get()
        if self._testLevel( message.getLevel() ):
          bundle.append( message.toTuple() )

      if len( bundle ):
        self._sendMessageToServer( bundle )

    if len( self._Transactions ):
      self._sendMessageToServer()

  def _sendMessageToServer( self, messageBundle=None ):
    from DIRAC.Core.DISET.RPCClient import RPCClient
    if messageBundle:
      self._Transactions.append( messageBundle )
    TransactionsLength = len( self._Transactions )
    if TransactionsLength > 100:
      del self._Transactions[:TransactionsLength-100]
      TransactionsLength = 100

    try:
      oSock = RPCClient( "Framework/SystemLogging" )
    except Exception,v:
      return False

    while TransactionsLength:
      result = oSock.addMessages( self._Transactions[0],
                                  self._site, self._hostname )
      if result['OK']:
        TransactionsLength = TransactionsLength - 1
        self._Transactions.pop(0) 
      else:
        return False
    return True

  def _testLevel( self, sLevel ):
    messageLevel = self._logLevels.getLevelValue( sLevel )
    return messageLevel <= self._negativeLevel or \
           messageLevel >= self._positiveLevel

  def flush( self ):
    self._alive = False
    if not self.__interactive and self._sendMessageToServer()['OK']:
      while not self._messageQueue.empty():
        self._bundleMessages()

