# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/backends/BaseBackend.py,v 1.7 2008/02/26 20:15:59 mseco Exp $
__RCSID__ = "$Id: BaseBackend.py,v 1.7 2008/02/26 20:15:59 mseco Exp $"
"""  This constitues the base class for the backends of the logger
"""
from DIRAC.Core.Utilities import Time

class BaseBackend:

  _showCallingFrame = True

  def __init__( self, optionsDictionary ):
    self._optionsDictionary = optionsDictionary

  def flush( self ):
    pass

  def doMessage( self ):
    raise Exception( "This function MUST be overloaded!!" )

  def composeString( self, messageObject ):
    messageName = "%s" % messageObject.getName()
    if messageObject.getSubSystemName():
      messageName += "/%s" % messageObject.getSubSystemName()

    if self._showCallingFrame and messageObject.getFrameInfo():
      messageName += "[%s]" % messageObject.getFrameInfo()
    timeToShow = Time.toString( messageObject.getTime() ).split('.')[0]
    lines = []
    for lineString in messageObject.getMessage().split( "\n" ):
      lines.append( "%sUTC %s %s: %s" % ( timeToShow,
                                           messageName,
                                           messageObject.getLevel().rjust( 5 ),
                                           lineString ) )
    return "\n".join(lines)
