# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/Message.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $
__RCSID__ = "$Id: Message.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $"

class Message:
  
  def __init__( self, systemName, level, time, msgText, variableText, frameInfo ):
    self.systemName = systemName
    self.level = level
    self.time = time
    self.msgText = msgText
    self.variableText = variableText
    self.frameInfo = frameInfo
    self.subSystemName = False
    
  def getName( self ):
    return self.systemName
  
  def setName( self, systemName ):
    self.systemName = systemName
    
  def getSubSystemName( self ):
    return self.subSystemName
  
  def setSubSystemName( self, subSystemName ):
    self.subSystemName = subSystemName
  
  def getLevel( self ):
    return self.level
  
  def getTime( self ):
    return self.time
  
  def getMessage( self ):
    messageString = "%s %s" % ( self.getFixedMessage(), self.getVariableMessage() )
    return messageString.strip()
    
  def getFixedMessage( self ):
    return self.msgText
  
  def getVariableMessage( self ):
    if self.variableText:
      return self.variableText
    else:
      return ""
  
  def getFrameInfo( self ):
    return self.frameInfo
  
  def __str__( self ):
    messageString = ""
    for lineString in self.getMessage().split( "\n" ):
      messageString += "%s %s %s: %s" % ( str( self.getTime() ),
                               self.getName(),
                               self.getLevel().rjust( 6 ), 
                               lineString )
    return messageString
  
