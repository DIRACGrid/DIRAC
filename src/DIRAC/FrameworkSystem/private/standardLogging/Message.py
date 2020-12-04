""" This is put here from the old logger

    The only use left is from SystemLoggingHandler service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


__RCSID__ = "$Id$"

from DIRAC.Core.Utilities import Time


def tupleToMessage(varTuple):
  varList = list(varTuple)
  varList[2] = Time.fromString(varList[2])
  return Message(*varList)


class Message:

  def __init__(self, systemName, level, time, msgText, variableText, frameInfo, subSystemName=''):
    from six.moves import _thread as thread
    self.systemName = systemName
    self.level = level
    self.time = time
    self.msgText = str(msgText)
    self.variableText = str(variableText)
    self.frameInfo = frameInfo
    self.subSystemName = subSystemName
    self.threadId = thread.get_ident()

  def getName(self):
    return self.systemName

  def setName(self, systemName):
    self.systemName = systemName

  def getSystemName(self):
    return self.systemName

  def setSystemName(self, systemName):
    self.systemName = systemName

  def getSubSystemName(self):
    return self.subSystemName

  def setSubSystemName(self, subSystemName):
    self.subSystemName = subSystemName

  def getLevel(self):
    return self.level

  def getTime(self):
    return self.time

  def getMessage(self):
    msg = self.getFixedMessage()
    varMsg = self.getVariableMessage()
    if varMsg:
      msg += ' ' + varMsg
    return msg

  def getFixedMessage(self):
    return self.msgText

  def getVariableMessage(self):
    if self.variableText:
      return self.variableText
    else:
      return ""

  def getFrameInfo(self):
    return self.frameInfo

  def __str__(self):
    messageString = ""
    for lineString in self.getMessage().split("\n"):
      messageString += "%s %s %s: %s" % (str(self.getTime()),
                                         self.getName(),
                                         self.getLevel().rjust(6),
                                         lineString)
    return messageString

  def toTuple(self):
    return (self.systemName,
            self.level,
            Time.toString(self.time),
            self.msgText,
            self.variableText,
            self.frameInfo,
            self.subSystemName
            )
