__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.Client.Logger import Logger


class SubSystemLogger(Logger):

  def __init__(self, subName, masterLogger, child=True):
    Logger.__init__(self)
    self.__child = child
    self._minLevel = masterLogger._minLevel
    for attrName in dir(masterLogger):
      attrValue = getattr(masterLogger, attrName)
      if isinstance(attrValue, basestring):
        setattr(self, attrName, attrValue)
    self.__masterLogger = masterLogger
    self._subName = subName

  def getSubName(self):
    """
    Return the name of the sublogger
    """
    return self._subName

  def processMessage(self, messageObject):
    if self.__child:
      messageObject.setSubSystemName(self._subName)
    else:
      messageObject.setSystemName(self._subName)


self.__masterLogger.processMessage(messageObject)
