"""  UserStorageQuotaAgent determines the current storage usage for each user.\
"""
from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
from DIRAC.Core.Utilities.Subprocess import shellCall

import time,os
from types import *
import lfc

AGENT_NAME = 'DataManagement/UserStorageQuotaAgent'

class UserStorageQuotaAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.lfc = FileCatalog(['LcgFileCatalogCombined'])    
    self.StorageUsageDB = StorageUsageDB()
    self.defaultQuota = float(gConfig.getValue('/Groups/lhcb/Quota'))
    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True')
    if self.useProxies == 'True':
      self.proxyDN = gConfig.getValue(self.section+'/ProxyDN','')
      self.proxyGroup = gConfig.getValue(self.section+'/ProxyGroup','')
      self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
      self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','')
      if os.path.exists(self.proxyLocation):
        os.remove(self.proxyLocation)
    return result

  def execute(self):

    if self.useProxies == 'True':
      ############################################################
      #
      # Get a valid proxy for the current activity
      #
      self.log.info("StorageUsageAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("StorageUsageAgent: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("StorageUsageAgent: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("StorageUsageAgent: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("StorageUsageAgent: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 6000:
          obtainProxy = True

      if obtainProxy:
        self.log.info("StorageUsageAgent: Attempting to renew %s proxy." %self.proxyDN)
        wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
        res = wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("StorageUsageAgent: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("StorageUsageAgent: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("StorageUsageAgent: Successfully renewed %s proxy." %self.proxyDN)

    res = self.StorageUsageDB.getUserStorageUsage()
    usageDict = res['Value']
    usageToUserDict = {}
    userNameDNDict = {}

    byteToMB = 1000*1000.0

    for userName in sortList(usageDict.keys()):
      usageMB = usageDict[userName]/byteToMB
      res = gConfig.getOptionsDict('/Users/%s' % userName)
      if not res['OK']:
        gLogger.error("UserStorageQuotaAgent: Username not found in the CS.",userName)
        userQuota = 0        
      else:
        if not res['Value'].has_key('Quota'):
          userQuota = self.defaultQuota
        else:
          userQuota = float(res['Value']['Quota'])


        #we probably want diffirent behaviour for 90% exceeded, 110% exceeded and 150% exceeded
        if (1.5*userQuota) < usageMB:
          gLogger.info("UserStorageQuotaAgent: %s is 50%s over quota %s: %s." % (userName,'%',userQuota,usageMB))
          userMail = res['Value']['mail']
          res = self.sendBlockedMail(userMail,userQuota,usageMB)
          if not res['OK']:
            gLogger.error("UserStorageQuotaAgent: Failed to send blocked email to user.",res['Message'])
          else:
            gLogger.info("UserStorageQuotaAgent: Sent blocked email to user.")
            gLogger.info("!!!!!!!!!!!!!!!!!!!!!!!!REMEMBER TO MODIFY THE ACLs and STATUS HERE!!!!!!!!!!!!!!!!!")
        elif (1.1*userQuota) < usageMB:
          gLogger.info("UserStorageQuotaAgent: %s is 10%s over quota %s: %s." % (userName,'%',userQuota,usageMB))
          userMail = res['Value']['mail']
          res = self.sendWarningMail(userMail,userQuota,usageMB)
          if not res['OK']:
            gLogger.error("UserStorageQuotaAgent: Failed to send warning email to user.",res['Message'])
          else:
            gLogger.info("UserStorageQuotaAgent: Sent warning email to user.")
        else:
          gLogger.info("UserStorageQuotaAgent: %s within quota %s: %s." % (userName,userQuota,usageMB))
    return S_OK()

  def sendWarningMail(self,userMail,quota,usage):
    try:
      import smtplib
      from email.MIMEText import MIMEText
      msgbody = "This mail has been generated automatically.\n\n"
      msgbody += "You have received this mail because your Grid storage usage has exceeded your quota of %sMB.\n\n" % int(quota)
      msgbody += "You are currently using %sMB.\n\n" % int(usage)
      msgbody += "Please reduce you usage by removing some files. If you have reduced your usage in the last 24 hours please ignore this message."    
      msg = MIMEText(msgbody)
      msg['From'] = 'Andrew C. Smith <a.smith@cern.ch>'
      msg['To'] = userMail
      msg['Subject'] = 'Grid Storage Quota'
      s = smtplib.SMTP()
      s.connect()
      s.sendmail('a.smith@cern.ch', [userMail], msg.as_string())
      return S_OK()
    except Exception,x:
      return S_ERROR(str(x))
 
  def sendBlockedMail(self,userMail,quota,usage):
    try:
      import smtplib
      from email.MIMEText import MIMEText
      msgbody = "This mail has been generated automatically.\n\n"
      msgbody += "You have received this mail because your Grid storage usage has exceeded your quota of %sMB.\n\n" % int(quota)
      msgbody += "You are currently using %sMB.\n\n" % int(usage)
      msgbody += "Your account has now been blocked and you will not be able to save more output without creating space. If you have reduced your usage in the last 24 hours please ignore this message."
      msg = MIMEText(msgbody)
      msg['From'] = 'Andrew C. Smith <a.smith@cern.ch>'
      msg['To'] = userMail
      msg['Subject'] = 'Grid Storage Quota'
      s = smtplib.SMTP()
      s.connect()
      s.sendmail('a.smith@cern.ch', [userMail], msg.as_string())
      return S_OK()
    except Exception,x:
      return S_ERROR(str(x))



    """
    gLogger.info("%s %s" % ('User'.ljust(30),'Total usage'.ljust(30)))
    lfnBase = '/lhcb/user'
    for userName in sortList(usageDict.keys()): 
      lfn = "%s/%s/%s" % (lfnBase,userName[0],userName)
      res = self.lfc.getDirectoryMetadata(lfn)
      if not res['OK']:
        gLogger.error("Failed to get the directory metadata:", res['Message'])
        userNameDNDict[userName] = userName
      elif not res['Value']['Successful'].has_key(lfn):
        gLogger.error("Failed to get metadata:", res['Value']['Failed'][lfn])
        userNameDNDict[userName] = userName
      else:
        lfnMetadata = res['Value']['Successful'][lfn]
        creatorDN = lfnMetadata['CreatorDN']
        userNameDNDict[userName] = creatorDN
             
      #gLogger.info("%s %s" % (userName.ljust(30),str(usageDict[userName]/byteToGB).ljust(30)))
      gLogger.info("%s %s %s" % (creatorDN.ljust(90),userName.ljust(30),str(usageDict[userName]/byteToGB).ljust(30)))
      usageToUserDict[usageDict[userName]] = userName

    gLogger.info("Top ten users.")
    gLogger.info("%s %s" % ('User'.ljust(30),'Total usage'.ljust(30)))
    for usage in sortList(usageToUserDict.keys(),True):
      gLogger.info("%s %s %s" % (userNameDNDict[usageToUserDict[usage]].ljust(90),usageToUserDict[usage].ljust(30),str(usage/byteToGB).ljust(30)))
    """
