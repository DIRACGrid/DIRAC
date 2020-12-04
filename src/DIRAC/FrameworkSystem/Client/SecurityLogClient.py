""" For reporting messages to security log service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import syslog

from DIRAC import gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler


class SecurityLogClient(object):

  __securityLogStore = []

  def __init__(self):
    self.__messagesList = []
    self.__maxMessagesInBundle = 1000
    self.__maxMessagesWaiting = 10000
    self.__taskId = gThreadScheduler.addPeriodicTask(30, self.__sendData)

  def addMessage(self, success, sourceIP, sourcePort, sourceIdentity,
                 destinationIP, destinationPort, destinationService,
                 action, timestamp=False):
    if not timestamp:
      timestamp = Time.dateTime()
    msg = [timestamp, success, sourceIP, sourcePort, sourceIdentity,
           destinationIP, destinationPort, destinationService, action]
    if gConfig.getValue("/Registry/EnableSysLog", False):
      strMsg = "Time=%s Accept=%s Source=%s:%s SourceID=%s Destination=%s:%s Service=%s Action=%s"
      syslog.syslog(strMsg % msg)
    while len(self.__messagesList) > self.__maxMessagesWaiting:
      self.__messagesList.pop(0)
    if not self.__securityLogStore:
      self.__messagesList.append(msg)
    else:
      self.__securityLogStore[0].logAction(msg)

  def setLogStore(self, logStore):
    while self.__securityLogStore:
      self.__securityLogStore.pop()
    self.__securityLogStore.append(logStore)
    gThreadScheduler.addPeriodicTask(10, self.__sendData, executions=1)

  def __sendData(self):
    gLogger.verbose("Sending records to security log service...")
    msgList = self.__messagesList
    self.__messagesList = []
    rpcClient = RPCClient("Framework/SecurityLogging")
    for _i in range(0, len(msgList), self.__maxMessagesInBundle):
      msgsToSend = msgList[:self.__maxMessagesInBundle]
      result = rpcClient.logActionBundle(msgsToSend)
      if not result['OK']:
        self.__messagesList.extend(msgList)
        break
      msgList = msgList[self.__maxMessagesInBundle:]
    gLogger.verbose("Data sent to security log service")
