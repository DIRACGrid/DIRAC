# $HeadURL$
import threading
__RCSID__ = "$Id$"
"""  This constitues the base class for the backends of the logger
"""

class BaseBackend:

  _charData = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  _showCallingFrame = True

  def __init__( self, optionsDictionary ):
    self._optionsDictionary = optionsDictionary

  def flush( self ):
    pass

  def doMessage( self ):
    raise Exception( "This function MUST be overloaded!!" )

  def getThreadId( self ):
    rid = ""
    thid = str( threading.current_thread().ident )
    segments = []
    for iP in range( len( thid ) ):
      if iP % 4 == 0:
        segments.append( "" )
      segments[-1] += thid[ iP ]
    for seg in segments:
      rid += BaseBackend._charData[ int( seg ) % len( BaseBackend._charData ) ]
    return rid


  def composeString( self, messageObject ):
    from DIRAC.Core.Utilities import Time
    #If not headers, just show lines
    if not self._optionsDictionary[ 'showHeaders' ]:
      return messageObject.getMessage()
    #Do the full header
    messageName = "%s" % messageObject.getName()
    if messageObject.getSubSystemName():
      messageName += "/%s" % messageObject.getSubSystemName()

    if self._showCallingFrame and messageObject.getFrameInfo():
      messageName += "[%s]" % messageObject.getFrameInfo()
    timeToShow = Time.toString( messageObject.getTime() ).split( '.' )[0]
    lines = []
    prefix = [ timeToShow, "UTC", messageName, "%s:" % messageObject.getLevel().rjust( 6 ) ]
    if self._optionsDictionary[ 'showThreads' ]:
      prefix[2] += "[%s]" % self.getThreadId()
    prefix = " ".join( prefix )
    for lineString in messageObject.getMessage().split( "\n" ):
      lines.append( "%s %s" % ( prefix, lineString ) )
    return "\n".join( lines )
