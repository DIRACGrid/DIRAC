
from DIRAC.Core.Utilities import Time
from DIRAC.LoggingSystem.private.backends.BaseBackend import BaseBackend

class PrintBackend( BaseBackend ):

  def doMessage( self, messageObject ):
    messageName = "%s" % messageObject.getName()
    if messageObject.getSubSystemName():
      messageName += "/%s" % messageObject.getSubSystemName()
    if self._showCallingFrame and messageObject.getFrame():
      messageName += "[%s]" % messageObject.getFrame()
    timeToShow = Time.toString( messageObject.getTime() ).split('.')[0]
    for lineString in messageObject.getMessage().split( "\n" ):
      print "%s %s %s: %s" % ( timeToShow,
                               messageName,
                               messageObject.getLevel().rjust( 5 ),
                               lineString )