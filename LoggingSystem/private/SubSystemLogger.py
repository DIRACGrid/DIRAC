# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/SubSystemLogger.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $
__RCSID__ = "$Id: SubSystemLogger.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $"
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.LoggingSystem.Client.Logger import Logger

class SubSystemLogger( Logger ):
  
  def __init__( self, subSystemName, masterLogger ):
    Logger.__init__( self, 1 )
    self._subSystemName = subSystemName
    self.__masterLogger = masterLogger
    
  def processMessage( self, messageObject ):
    messageObject.setSubSystemName( self._subSystemName )
    self.__masterLogger.processMessage( messageObject )
