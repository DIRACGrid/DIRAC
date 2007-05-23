
import threading
import Queue
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend

class RemoteBackend( threading.Thread ):

  def __init__( self ):
    threading.Thread.__init__( self )
    self._msgQueue = Queue.Queue()
    self._alive = True
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._msgQueue.push( messageObject )

  def run( self ):
    while self._alive:
      msgObject = self._msgQueue.get()
      self._sendMessageToServer( msgObject )

  def _sendMessageToServer( self, msgObject ):
    #TODO: Here the message shoud be sent to the logging server
    pass
