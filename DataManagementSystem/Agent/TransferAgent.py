"""  TransferAgent takes transfer requests from the RequestDB and replicates them
"""

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import time,os
from types import *

AGENT_NAME = 'DataManagement/TransferAgent'

class TransferAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()

    gMonitor.registerActivity( "Iteration", "Agent Loops/min",          "TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Execute",   "Request Processed/min",    "TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Done",      "Request Completed/min",    "TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replicate and register","Replicate and register operations attempted/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replication successful","Successful replications/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replication failed","Failed replications/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replica registration successful","Successful replica registrations/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replica registration failed","Failed replica registrations/min","TransferAgent", "Atemps", gMonitor.OP_SUM )  

    self.threadPool = ThreadPool(1,1)

    self.useProxies = gConfig.getValue(self.section+'/UseProxies',False)
    if self.useProxies:
      self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
      self.proxyDN = gConfig.getValue(self.section+'/ProxyDN','')
      self.proxyGroup = gConfig.getValue(self.section+'/ProxyGroup','')
      self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
      self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','')

    return result

  def execute(self):

    if self.useProxies:
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

    for i in range(1):
      requestExecutor = ThreadedJob(self.executeRequest)
      self.threadPool.queueJob(requestExecutor)
    self.threadPool.processResults()
    return self.executeRequest()

  def executeRequest(self):
    ################################################
    # Get a request from request DB
    gMonitor.addMark( "Iteration", 1 )
    res = self.RequestDBClient.getRequest('transfer')
    if not res['OK']:
      gLogger.info("TransferAgent.execute: Failed to get request from database.")
      return S_OK()
    elif not res['Value']:
      gLogger.info("TransferAgent.execute: No requests to be executed found.")
      return S_OK()
    requestString = res['Value']['requestString']
    requestName = res['Value']['requestName']
    sourceServer= res['Value']['Server']
    gLogger.info("TransferAgent.execute: Obtained request %s" % requestName)
    oRequest = DataManagementRequest(request=requestString)

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('transfer')
    if not res['OK']:
      errStr = "TransferAgent.execute: Failed to obtain number of transfer subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    gLogger.info("TransferAgent.execute: Found %s sub requests." % res['Value'])

    ################################################
    # For all the sub-requests in the request
    for ind in range(res['Value']):
      gMonitor.addMark( "Execute", 1 )
      gLogger.info("TransferAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'transfer')['Value']
      if subRequestAttributes['Status'] == 'Waiting':
        subRequestFiles = oRequest.getSubRequestFiles(ind,'transfer')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a put and register operation
        if operation == 'putAndRegister':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile['LFN']
              file = subRequestFile['PFN']
              guid = subRequestFile['GUID']
              res = self.ReplicaManager.putAndRegister(lfn, file, diracSE, guid=guid)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  if not res['Value']['Successful'][lfn].has_key('put'):
                    gLogger.info("TransferAgent.execute: Failed to put %s to %s." % (lfn,diracSE))
                  elif not res['Value']['Successful'][lfn].has_key('register'):
                    gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['put']))
                    gLogger.info("TransferAgent.execute: Failed to register %s to %s." % (lfn,diracSE))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                    fileDict = res['Value']['Failed'][lfn]['register']
                    registerRequestDict = {'Attributes':{'TargetSE': fileDict['TargetSE'],'Operation':'registerFile'},'Files':[{'LFN': fileDict['LFN'],'PFN':fileDict['PFN'], 'Size':fileDict['Size'], 'GUID':fileDict['GUID']}]}
                    gLogger.info("TransferAgent.execute: Setting registration request for failed file.")
                    oRequest.addSubRequest(registerRequestDict,'register')
                  else:
                    gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['put']))
                    gLogger.info("TransferAgent.execute: Successfully registered %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['register']))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                else:
                  errStr = "TransferAgent.execute: Failed to put and register file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,diracSE,res['Value']['Failed'][lfn]))
              else:
                errStr = "TransferAgent.execute: Completely failed to put and register file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is a put operation
        elif operation == 'put':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile['LFN']
              file = subRequestFile['PFN']
              res = self.ReplicaManager.put(lfn, file, diracSE)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]))
                  oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                else:
                  errStr = "TransferAgent.execute: Failed to put file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,diracSE,res['Value']['Failed'][lfn]))
              else:
                errStr = "TransferAgent.execute: Completely failed to put file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is a replicate and register operation
        elif operation == 'replicateAndRegister':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          targetSE = subRequestAttributes['TargetSE']
          sourceSE = subRequestAttributes['SourceSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              gMonitor.addMark("Replicate and register",1)
              lfn = subRequestFile['LFN']
              res = self.ReplicaManager.replicateAndRegister(lfn,targetSE,sourceSE=sourceSE)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  if not res['Value']['Successful'][lfn].has_key('replicate'):
                    gLogger.info("TransferAgent.execute: Failed to replicate %s to %s." % (lfn,targetSE))
                    gMonitor.addMark("Replication failed",1)
                  elif not res['Value']['Successful'][lfn].has_key('register'):
                    gMonitor.addMark("Replication successful",1)
                    gMonitor.addMark("Replica registration failed",1)
                    gLogger.info("TransferAgent.execute: Successfully replicated %s to %s in %s seconds." % (lfn,targetSE,res['Value']['Successful'][lfn]['replicate']))
                    gLogger.info("TransferAgent.execute: Failed to register %s to %s." % (lfn,targetSE))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                    fileDict = res['Value']['Failed'][lfn]['register']
                    registerRequestDict = {'Attributes':{'TargetSE': fileDict['TargetSE'],'Operation':'registerReplica'},'Files':[{'LFN': fileDict['LFN'],'PFN':fileDict['PFN']}]}
                    gLogger.info("TransferAgent.execute: Setting registration request for failed replica.")
                    oRequest.addSubRequest(registerRequestDict,'register')
                  else:
                    gMonitor.addMark("Replication successful",1)
                    gMonitor.addMark("Replica registration successful",1)
                    gLogger.info("TransferAgent.execute: Successfully replicated %s to %s in %s seconds." % (lfn,targetSE,res['Value']['Successful'][lfn]['replicate']))
                    gLogger.info("TransferAgent.execute: Successfully registered %s to %s in %s seconds." % (lfn,targetSE,res['Value']['Successful'][lfn]['register']))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                else:
                  errStr = "TransferAgent.execute: Failed to replicate and register file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,targetSE,res['Value']['Failed'][lfn]))
              else:
                errStr = "TransferAgent.execute: Completely failed to replicate and register file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is a replicate operation
        elif operation == 'replicate':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          targetSE = subRequestAttributes['TargetSE']
          sourceSE = subRequestAttributes['SourceSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile['LFN']
              res = self.ReplicaManager.replicate(lfn,targetSE,sourceSE=sourceSE)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  gLogger.info("TransferAgent.execute: Successfully replicated %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]))
                  oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                else:
                  errStr = "TransferAgent.execute: Failed to replicate file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,targetSE,res['Value']['Failed'][lfn]))
              else:
                errStr = "TransferAgent.execute: Completely failed to replicate file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.error("TransferAgent.execute: Operation not supported.", operation)

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty(ind,'transfer')['Value']:
          oRequest.setSubRequestStatus(ind,'transfer','Done')
          gMonitor.addMark( "Done", 1 )

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info("TransferAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,sourceServer)

    return S_OK()
