import threading
import Queue
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message

class FileBackend( BaseBackend, threading.Thread ):
  def __init__( self, cfgPath ):
    threading.Thread.__init__( self )
    self._backendName = "file"
    self._msgQueue = Queue.Queue()
    self._alive = True
    self._minLevel = 'ERROR'
    self.filename = 'log_generic.out'
#    try:
#      wlock[ 'log_generic.out' ] = threading.Lock()
#    except:
#      print 'Could not create lock'
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._msgQueue.put( messageObject )


  def run( self ):
    while self._alive:
      try:
        messageObject = self._msgQueue.get()
        self.doWrite( self.composeString( messageObject ) )
      except:
        print 'An error ocurred'

  def doWrite( self, sLine ):
    try:
      self.file=open( self.filename, 'a' )
    except:
      print 'Could not open file %s ' % self.filename

    self.file.write( "%s\n" % sLine )

    self.file.close()

    return True
