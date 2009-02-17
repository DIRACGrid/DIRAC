# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/SubSystemLogger.py,v 1.7 2009/02/17 16:45:23 acasajus Exp $
__RCSID__ = "$Id: SubSystemLogger.py,v 1.7 2009/02/17 16:45:23 acasajus Exp $"
import types
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.LoggingSystem.Client.Logger import Logger

class SubSystemLogger( Logger ):

  def __init__( self, subName, masterLogger, child = True ):
    Logger.__init__( self )
    self.__child = child
    for attrName in dir( masterLogger ):
      attrValue = getattr( masterLogger, attrName )
      if type( attrValue ) == types.StringType:
        setattr( self, attrName, attrValue )
    self.__masterLogger = masterLogger
    self._subName = subName

  def processMessage( self, messageObject ):
    if self.__child:
      messageObject.setSubSystemName( self._subName )
    else:
      messageObject.setSystemName( self._subName )
    self.__masterLogger.processMessage( messageObject )
