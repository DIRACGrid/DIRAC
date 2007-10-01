from DIRAC.Core.Utilities import Time

class BaseBackend:

  _showCallingFrame = False

  def __init__( self, cfgPath ):
    self.cfgPath = cfgPath

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
      lines.append( "%s %s %s: %s" % ( timeToShow,
                                           messageName,
                                           messageObject.getLevel().rjust( 5 ),
                                           lineString ) )
    return "\n".join(lines)