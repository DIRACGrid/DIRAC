
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend

class PrintBackend( BaseBackend ):

  def doMessage( self, messageObject ):
    print self.composeString( messageObject )

