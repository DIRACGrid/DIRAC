"""  UserStorageQuotaAgent obtains the usage by each user from the StorageUsageDB and compares with a quota present in the CS.
"""
# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

import time,os
from types import *

AGENT_NAME = 'DataManagement/UserStorageQuotaAgent'

class UserStorageQuotaAgent(AgentModule):

  def initialize(self):
    if self.am_getOption('DirectDB',False):
      from DIRAC.DataManagement.DB.StorageUsageDB import StorageUsageDB
      self.StorageUsageDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.StorageUsageDB = RPCClient('DataManagement/StorageUsage')
    self.am_setModuleParam("shifterProxy", "DataManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    self.defaultQuota = gConfig.getValue('/Security/DefaultStorageQuota',1000) # Default is 1TB
    gLogger.info("initialize: Default quota found to be %d GB" % self.defaultQuota)
    return S_OK()

  def execute(self):
    res = self.StorageUsageDB.getUserStorageUsage()
    usageDict = res['Value']
    usageToUserDict = {}
    userNameDNDict = {}

    byteToGB = 1000*1000*1000.0

    gLogger.info("Determining quota usage for %s users." % len(usageDict.keys()))
    for userName in sortList(usageDict.keys()):
      usageGB = usageDict[userName]/byteToGB
      res = gConfig.getOptionsDict('/Security/Users/%s' % userName)
      if not res['OK']:
        gLogger.error("Username not found in the CS.","%s using %.2f GB" % (userName,usageGB))
        continue
      elif not res['Value'].has_key('email'):
        gLogger.error("CS does not contain email information for user.",userName)
        continue
      elif not res['Value'].has_key('Quota'):
        userQuota = float(self.defaultQuota)
      else:
        userQuota = float(res['Value']['Quota'])
      userMail = res['Value']['email']
      # Diffirent behaviour for 90% exceeded, 110% exceeded and 150% exceeded
      if (1.5*userQuota) < usageGB:      
        gLogger.info("%s is at %d%s of quota %d GB (%.1f GB)." % (userName,(usageGB*100)/userQuota,'%',userQuota,usageGB))
        self.sendBlockedMail(userName,userMail,userQuota,usageGB)
        gLogger.info("!!!!!!!!!!!!!!!!!!!!!!!!REMEMBER TO MODIFY THE ACLs and STATUS HERE!!!!!!!!!!!!!!!!!")
      elif (1.0*userQuota) < usageGB:
        gLogger.info("%s is at %d%s of quota %d GB (%.1f GB)." % (userName,(usageGB*100)/userQuota,'%',userQuota,usageGB))
        self.sendSecondWarningMail(userName,userMail,userQuota,usageGB)
      elif (0.9*userQuota) < usageGB:
        gLogger.info("%s is at %d%s of quota %d GB (%.1f GB)." % (userName,(usageGB*100)/userQuota,'%',userQuota,usageGB))
        self.sendFirstWarningMail(userName,userMail,userQuota,usageGB)
    return S_OK()

  def sendFirstWarningMail(self,userName,userMail,quota,usage):
    msgbody = "This mail has been generated automatically.\n\n"
    msgbody += "You have received this mail because you are approaching your Grid storage usage quota of %sGB.\n\n" % int(quota)
    msgbody += "You are currently using %.1f GB.\n\n" % usage
    msgbody += "Please reduce you usage by removing some files. If you have reduced your usage in the last 24 hours please ignore this message."
    fromAddress = 'LHCb Data Manager <lhcb-datamanagement@cern.ch>'
    subject = 'Grid storage use near quota (%s)' % userName 
    toAddress = userMail
    NotificationClient().sendMail(toAddress, subject, msgbody, fromAddress)

  def sendSecondWarningMail(self,userName,userMail,quota,usage):
    msgbody = "This mail has been generated automatically.\n\n"
    msgbody += "You have received this mail because your Grid storage usage has exceeded your quota of %sGB.\n\n" % int(quota)
    msgbody += "You are currently using %.1f GB.\n\n" % usage
    msgbody += "Please reduce you usage by removing some files. If you have reduced your usage in the last 24 hours please ignore this message."
    fromAddress = 'LHCb Data Manager <lhcb-datamanagement@cern.ch>'
    subject = 'Grid storage use over quota (%s)' % userName
    toAddress =  userMail
    NotificationClient().sendMail(toAddress, subject, msgbody, fromAddress)

  def sendBlockedMail(self,userName,userMail,quota,usage):
    msgbody = "This mail has been generated automatically.\n\n"
    msgbody += "You have received this mail because your Grid storage usage has exceeded your quota of %sGB.\n\n" % int(quota)
    msgbody += "You are currently using %.1f GB.\n\n" % usage
    msgbody += "Your account has now been blocked and you will not be able to save more output without creating space. If you have reduced your usage in the last 24 hours please ignore this message."
    fromAddress = 'LHCb Data Manager <lhcb-datamanagement@cern.ch>'
    subject = 'Grid storage use blocked (%s)' % userName
    toAddress = userMail
    NotificationClient().sendMail(toAddress, subject, msgbody, fromAddress) 
