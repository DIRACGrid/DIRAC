"""  FTS Submit Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,setDIRACGroup,getProxyTimeLeft
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import os,time
from types import *

AGENT_NAME = 'DataManagement/FTSRegister'

class FTSRegister(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.TransferDB = TransferDB()
    self.ReplicaManager = ReplicaManager()
    self.DataLog = RPCClient('DataManagement/DataLogging')

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True')
    if self.useProxies == 'True':
      self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
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
      self.log.info("TransferAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("TransferAgent.execute: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("TransferAgent.execute: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("TransferAgent.execute: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("TransferAgent.execute: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 60:
          obtainProxy = True

      if obtainProxy:
        self.log.info("TransferAgent.execute: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("TransferAgent.execute: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("TransferAgent.execute: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("TransferAgent.execute: Successfully renewed %s proxy." %self.proxyDN)

    res = self.TransferDB.getWaitingRegistrations()
    if not res['OK']:
      gLogger.error("FTSRegister.execute: Failed to get waiting registrations from TransferDB.",res['Message'])
      return S_OK()
    lfns = {}
    replicaTuples = []
    for fileID,channelID,lfn,pfn,se in res['Value']:
      lfns[lfn] = (channelID,fileID,se)
      replicaTuples.append((lfn,pfn,se))
    if replicaTuples:
      gLogger.info("FTSRegister.execute: Found  %s waiting replica registrations." % len(replicaTuples))
      res = self.ReplicaManager.registerReplica(replicaTuples)
      if not res['OK']:
        gLogger.error("FTSRegister.execute: Completely failed to regsiter replicas.",res['Message'])
        return S_OK()
      channelID,fileID,se = lfns[lfn]
      for lfn in res['Value']['Successful'].keys():
        self.DataLog.addFileRecord(lfn,'Register',se,'','FTSRegisterAgent')
        self.TransferDB.setRegistrationDone(channelID,fileID)
      for lfn in res['Value']['Failed'].keys():
        self.DataLog.addFileRecord(lfn,'RegisterFailed',se,'','FTSRegisterAgent') 
    else:
      gLogger.info("FTSRegister.execute: No waiting registrations found.")
    return S_OK()
