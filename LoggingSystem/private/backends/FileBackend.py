# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/FileBackend.py,v 1.2 2008/01/24 19:04:32 mseco Exp $
__RCSID__ = "$Id: FileBackend.py,v 1.2 2008/01/24 19:04:32 mseco Exp $"
"""  This backend writes the log messages to a file
"""
import threading
import Queue
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message

class FileBackend( BaseBackend, threading.Thread ):
  def __init__( self, optionsDictionary ):
    threading.Thread.__init__( self )
    self._backendName = "file"
    self._messageQueue = Queue.Queue()
    self._alive = True
    self._minLevel = 'ERROR'
    try:
      self._filename = optionsDictionary[ 'FileName' ]
    except:
      self._filename = 'SystemLoggingService.log'
    self.setDaemon(1)
    self.start()

  def doMessage( self, messageObject ):
    self._messageQueue.put( messageObject )


  def run( self ):
    while self._alive:
      try:
        messageObject = self._messageQueue.get()
        self.doWrite( self.composeString( messageObject ) )
      except:
        print 'An error ocurred'

  def doWrite( self, sLine ):
    try:
      self.file=open( self._filename, 'a' )
    except:
      print 'Could not open file %s ' % self.filename
      return False

    self.file.write( "%s\n" % sLine )

    self.file.close()

    return True
