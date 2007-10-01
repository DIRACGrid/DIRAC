import threading
import Queue
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.PrintBackend import PrintBackend
from DIRAC.LoggingSystem.private.LogLevels import LogLevels

class RemoteBackend( PrintBackend, threading.Thread ):

  def __init__( self, cfgPath ):
    threading.Thread.__init__( self )
    self._msgQueue = Queue.Queue()
    self._alive = True
#    self._running= False
    self._logLevels = LogLevels()
    self._maxBundledMsgs = 20
    self._minLevel = self._logLevels.getLevelValue( 'ALWAYS' )
    self.config()
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._msgQueue.put( messageObject )

  def run( self ):
    while self._alive:
      bundle = []
      msg = self._msgQueue.get()
      if self._testLevel( msg.getLevel() ):
        bundle.append( msg.toTuple() )
      while len( bundle ) < self._maxBundledMsgs:
        if not self._msgQueue.empty():
          msg = self._msgQueue.get()
          if self._testLevel( msg.getLevel() ):
            bundle.append( msg.toTuple() )
        else:
          break     
      if len( bundle ) > 0:
        self._sendMessageToServer( bundle )
        
      
  def _sendMessageToServer( self, msgBundle ):
    self.oSock.doSomethingForEveryone( msgBundle )

  def config(self):
    from DIRAC.Core.DISET.RPCClient import RPCClient
    self.oSock = RPCClient( "Logging/Logging" )
    
# To implement this part we will have to make use of locks.
# For the time beeing it is not neccessary so in order to avoid
# problems it is commented out.
#
#  def unset(self):
#    if self._running:
#      self._running=False
#      self.oSock.close()

  def _testLevel( self, sLevel ):
    return abs( self._logLevels.getLevelValue( sLevel ) ) >= self._minLevel

  def flush(self):
    while not self._msgQueue.empty():
      import time  
      time.sleep( .1 )