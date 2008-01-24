# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/RemoteBackend.py,v 1.10 2008/01/24 19:04:32 mseco Exp $
__RCSID__ = "$Id: RemoteBackend.py,v 1.10 2008/01/24 19:04:32 mseco Exp $"
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
    threading.Thread.__init__( self )
    self._messageQueue = Queue.Queue()
    self._alive = True
    self._site = optionsDictionary[ 'Site' ]
    self._domainName = getfqdn()
    self._logLevels = LogLevels()
    self._negativeLevel = self._logLevels.getLevelValue( 'ERROR' ) 
    self._positiveLevel = self._logLevels.getLevelValue( 'ALWAYS' )
    self._maxBundledMessages = 20
    self.config()
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._messageQueue.put( messageObject )

  def run( self ):
    while self._alive:
      bundle = []
      message = self._messageQueue.get()
      if self._testLevel( message.getLevel() ):
        bundle.append( message.toTuple() )
      while len( bundle ) < self._maxBundledMessages:
        if not self._messageQueue.empty():
          message = self._messageQueue.get()
          Level=message.getLevel()
          if self._testLevel( message.getLevel() ):
            bundle.append( message.toTuple() )
        else:
          break
      if len( bundle ) > 0:
        self._sendMessageToServer( bundle )


  def _sendMessageToServer( self, messageBundle ):
    self.oSock.addMessages( messageBundle, self._site, self._domainName )

  def config(self):
    from DIRAC.Core.DISET.RPCClient import RPCClient
    self.oSock = RPCClient( "Logging/SystemLogging", timeout = 10, useCertificates = "auto" )

  def _testLevel( self, sLevel ):
    
    return self._logLevels.getLevelValue( sLevel ) <= self._negativeLevel or \
           self._logLevels.getLevelValue( sLevel ) >= self._positiveLevel

  def flush(self):
    while not self._messageQueue.empty():
      import time
      time.sleep( .1 )
