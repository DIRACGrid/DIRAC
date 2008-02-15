# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/FileBackend.py,v 1.3 2008/02/15 17:45:06 mseco Exp $
__RCSID__ = "$Id: FileBackend.py,v 1.3 2008/02/15 17:45:06 mseco Exp $"
"""  This backend writes the log messages to a file
"""
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend

class FileBackend( BaseBackend ):
  def __init__( self, optionsDictionary ):
    self._backendName = "file"
    self._filename = optionsDictionary[ 'FileName' ]

  def doMessage( self, messageObject ):
    try:
      self.file=open( self._filename, 'a' )
    except Exception, v:
      print 'Could not open file %s ' % self._filename
      return 

    self.file.write( "%s\n" % self.composeString( messageObject ) )

    self.file.close()


