# $HeadURL$
__RCSID__ = "$Id$"
"""  This constitues the base class for the backends of the logger
"""

class BaseBackend:

  _showCallingFrame = True

  def __init__( self, optionsDictionary ):
    self._optionsDictionary = optionsDictionary

  def flush( self ):
    pass

  def doMessage( self ):
    raise Exception( "This function MUST be overloaded!!" )

  def composeString( self, messageObject ):
    from DIRAC.Core.Utilities import Time
    messageName = "%s" % messageObject.getName()
    if messageObject.getSubSystemName():
      messageName += "/%s" % messageObject.getSubSystemName()

    if self._showCallingFrame and messageObject.getFrameInfo():
      messageName += "[%s]" % messageObject.getFrameInfo()
    timeToShow = Time.toString( messageObject.getTime() ).split('.')[0]
    lines = []
    for lineString in messageObject.getMessage().split( "\n" ):
      lines.append( "%s UTC %s %s: %s" % ( timeToShow,
                                           messageName,
                                           messageObject.getLevel().rjust( 5 ),
                                           lineString ) )
    return "\n".join(lines)
