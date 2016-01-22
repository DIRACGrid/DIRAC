import sys
from DIRAC.FrameworkSystem.private.logging.backends.BaseBackend import BaseBackend

class StdErrBackend( BaseBackend ):

  def doMessage( self, messageObject ):
    sys.stderr.write( "%s\n" % self.composeString( messageObject ) )
    sys.stderr.flush()