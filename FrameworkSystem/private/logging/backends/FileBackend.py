# $HeadURL$
__RCSID__ = "$Id$"
"""  This backend writes the log messages to a file
"""
from DIRAC.FrameworkSystem.private.logging.backends.BaseBackend import BaseBackend

class FileBackend( BaseBackend ):
  def __init__( self, optionsDictionary ):
    super(FileBackend, self).__init__(optionsDictionary)
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


