"""  FTS Submit Agent takes files from the TransferDB and submits them to the FTS
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
import os,time,re
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
    self.DataLog = DataLoggingClient()

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    return result

  def execute(self):

    if self.useProxies:
      result = setupShifterProxyInEnv( "DataManager", self.proxyLocation )
      if not result[ 'OK' ]:
        self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return result

    res = self.TransferDB.getCompletedReplications()
    if not res['OK']:
      gLogger.error("FTSRegister.execute: Failed to get the completed replications from TransferDB.",res['Message'])
      return S_OK()
    filesToRemove = {}
    for operation,sourceSE,lfn in res['Value']:
      # This should get us the files that were supposed to be moved
      if re.search('ralmigration',operation.lower()):
        if not filesToRemove.has_key(sourceSE):
          filesToRemove[sourceSE] = []
        filesToRemove[sourceSE].append(lfn)

    for sourceSE,lfns in filesToRemove.items():
      #tmp hack until the ral people sort it out!!
      from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
      from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
      import time
      client = RequestClient()
      oRequest = DataManagementRequest()
      subRequestIndex = oRequest.initiateSubRequest('removal')['Value']
      attributeDict = {'Operation':'replicaRemoval','TargetSE':sourceSE}
      oRequest.setSubRequestAttributes(subRequestIndex,'removal',attributeDict)
      filesList = []
      for lfn in lfns:
        filesList.append({'LFN':lfn})
      oRequest.setSubRequestFiles(subRequestIndex,'removal',filesList)
      requestString = oRequest.toXML()['Value']
      requestName = 'RAL-removal-FTSRegister.%s' % time.time()
      res = client.setRequest(requestName,requestString,'dips://volhcb03.cern.ch:9143/RequestManagement/RequestManager')
      print res['OK']
      """
      gLogger.info("FTSRegister.execute:  Attemping to remove %s file(s) from %s." % (len(lfns),sourceSE))
      print lfns[0]
      res = self.ReplicaManager.removeReplica(sourceSE,lfns)
      if not res['OK']:
        gLogger.error("FTSRegister.execute: Completely failed to remove replicas.",res['Message'])
        return S_OK()
      for lfn in res['Value']['Successful'].keys():
        print 'successful'
        self.DataLog.addFileRecord(lfn,'RemoveReplica',sourceSE,'','FTSRegisterAgent')
      """

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
      for lfn in res['Value']['Successful'].keys():
        channelID,fileID,se = lfns[lfn]
        self.TransferDB.setRegistrationDone(channelID,fileID)
        self.DataLog.addFileRecord(lfn,'Register',se,'','FTSRegisterAgent')
    else:
      gLogger.info("FTSRegister.execute: No waiting registrations found.")
    return S_OK()
