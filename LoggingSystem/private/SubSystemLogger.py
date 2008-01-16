# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/SubSystemLogger.py,v 1.5 2008/01/16 11:46:05 acasajus Exp $
__RCSID__ = "$Id: SubSystemLogger.py,v 1.5 2008/01/16 11:46:05 acasajus Exp $"
import types
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.LoggingSystem.Client.Logger import Logger

class SubSystemLogger( Logger ):

  def __init__( self, subSystemName, masterLogger ):
    Logger.__init__( self )
    for attrName in dir( masterLogger ):
      attrValue = getattr( masterLogger, attrName )
      if type( attrValue ) == types.StringType:
        setattr( self, attrName, attrValue )
    self.__masterLogger = masterLogger
    self._subSystemName = subSystemName

  def processMessage( self, messageObject ):
    messageObject.setSubSystemName( self._subSystemName )
    self.__masterLogger.processMessage( messageObject )
