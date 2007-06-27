# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/SubSystemLogger.py,v 1.3 2007/06/27 16:05:18 acasajus Exp $
__RCSID__ = "$Id: SubSystemLogger.py,v 1.3 2007/06/27 16:05:18 acasajus Exp $"
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.LoggingSystem.Client.Logger import Logger

class SubSystemLogger( Logger ):

  def __init__( self, subSystemName, masterLogger ):
    Logger.__init__( self )
    self._subSystemName = subSystemName
    self.__masterLogger = masterLogger

  def queueMessage( self, messageObject ):
    messageObject.setSubSystemName( self._subSystemName )
    self.__masterLogger.queueMessage( messageObject )
