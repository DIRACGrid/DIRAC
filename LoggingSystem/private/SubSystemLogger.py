# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/SubSystemLogger.py,v 1.6 2008/12/01 11:47:08 acasajus Exp $
__RCSID__ = "$Id: SubSystemLogger.py,v 1.6 2008/12/01 11:47:08 acasajus Exp $"
import types
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.LoggingSystem.Client.Logger import Logger

class SubSystemLogger( Logger ):

  def __init__( self, systemName, masterLogger, child = True ):
    Logger.__init__( self )
    self.__child = child
    for attrName in dir( masterLogger ):
      attrValue = getattr( masterLogger, attrName )
      if type( attrValue ) == types.StringType:
        setattr( self, attrName, attrValue )
    self.__masterLogger = masterLogger
    self._systemName = systemName

  def processMessage( self, messageObject ):
    if self.__child:
      messageObject.setSubSystemName( self._systemName )
    else:
      messageObject.setSystemName( self._systemName )
    self.__masterLogger.processMessage( messageObject )
