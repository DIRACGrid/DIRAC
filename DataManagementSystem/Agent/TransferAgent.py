# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Agent/TransferAgent.py,v 1.27 2009/03/12 08:55:16 acsmith Exp $

"""  TransferAgent takes transfer requests from the RequestDB and replicates them
"""

__RCSID__ = "$Id: TransferAgent.py,v 1.27 2009/03/12 08:55:16 acsmith Exp $"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn

import time,os
from types import *

AGENT_NAME = 'DataManagement/TransferAgent'

class TransferAgent(Agent,RequestAgentMixIn):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.DataLog = DataLoggingClient()

    gMonitor.registerActivity( "Iteration", "Agent Loops",          "TransferAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Execute",   "Request Processed",    "TransferAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Done",      "Request Completed",    "TransferAgent", "Requests/min", gMonitor.OP_SUM )

    gMonitor.registerActivity("Replicate and register","Replicate and register operations","TransferAgent", "Attempts/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("Replicate","Replicate operations","TransferAgent", "Attempts/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("Put and register","Put and register operations","TransferAgent", "Attempts/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("Put","Put operations","TransferAgent", "Attempts/min", gMonitor.OP_SUM )

    gMonitor.registerActivity("Replication successful","Successful replications","TransferAgent", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("Put successful","Successful puts","TransferAgent", "Successful/min", gMonitor.OP_SUM )

    gMonitor.registerActivity("Replication failed","Failed replications","TransferAgent", "Failed/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("Put failed","Failed puts","TransferAgent", "Failed/min", gMonitor.OP_SUM )

    gMonitor.registerActivity("Replica registration successful","Successful replica registrations","TransferAgent", "Successful/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("File registration successful","Successful file registrations","TransferAgent", "Successful/min", gMonitor.OP_SUM )

    gMonitor.registerActivity("Replica registration failed","Failed replica registrations","TransferAgent", "Failed/min", gMonitor.OP_SUM )
    gMonitor.registerActivity("File registration failed","Failed file registrations","TransferAgent", "Failed/min", gMonitor.OP_SUM )

    self.maxNumberOfThreads = gConfig.getValue(self.section+'/NumberOfThreads',1)
    self.threadPoolDepth = gConfig.getValue(self.section+'/ThreadPoolDepth',1)
    self.threadPool = ThreadPool(1,self.maxNumberOfThreads)

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

    for i in range(self.threadPoolDepth):
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
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    sourceServer= res['Value']['Server']
    try:
      jobID = int(res['Value']['JobID'])
    except:
      jobID = 0
    gLogger.info("TransferAgent.execute: Obtained request %s" % requestName)

    result = self.RequestDBClient.getCurrentExecutionOrder(requestName,sourceServer)
    if result['OK']:
      currentOrder = result['Value']
    else:
      return S_OK('Can not get the request execution order')

    oRequest = RequestContainer(request=requestString)

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
    modified = False
    for ind in range(res['Value']):
      gMonitor.addMark( "Execute", 1 )
      gLogger.info("TransferAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'transfer')['Value']
      subExecutionOrder = int(subRequestAttributes['ExecutionOrder'])
      subStatus = subRequestAttributes['Status']
      if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
        subRequestFiles = oRequest.getSubRequestFiles(ind,'transfer')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a put and register operation
        if operation == 'putAndRegister' or operation == 'putAndRegisterAndRemove':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = str(subRequestAttributes['TargetSE'])
          catalog = ''
          if  subRequestAttributes.has_key('Catalogue'):
            catalog = subRequestAttributes['Catalogue']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              gMonitor.addMark("Put and register",1)
              lfn = str(subRequestFile['LFN'])
              file = subRequestFile['PFN']
              guid = subRequestFile['GUID']
              addler = subRequestFile['Addler']
              res = self.ReplicaManager.putAndRegister(lfn, file, diracSE, guid=guid,checksum=addler,catalog=catalog)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  if not res['Value']['Successful'][lfn].has_key('put'):
                    gMonitor.addMark("Put failed",1)
                    self.DataLog.addFileRecord(lfn,'PutFail',diracSE,'','TransferAgent')
                    gLogger.info("TransferAgent.execute: Failed to put %s to %s." % (lfn,diracSE))
                  elif not res['Value']['Successful'][lfn].has_key('register'):
                    gMonitor.addMark("Put successful",1)
                    gMonitor.addMark("File registration failed",1)
                    self.DataLog.addFileRecord(lfn,'Put',diracSE,'','TransferAgent')
                    self.DataLog.addFileRecord(lfn,'RegisterFail',diracSE,'','TransferAgent')
                    gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['put']))
                    gLogger.info("TransferAgent.execute: Failed to register %s to %s." % (lfn,diracSE))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                    fileDict = res['Value']['Failed'][lfn]['register']
                    registerRequestDict = {'Attributes':{'TargetSE': fileDict['TargetSE'],'Operation':'registerFile'},'Files':[{'LFN': fileDict['LFN'],'PFN':fileDict['PFN'], 'Size':fileDict['Size'], 'Addler':fileDict['Addler'], 'GUID':fileDict['GUID']}]}
                    gLogger.info("TransferAgent.execute: Setting registration request for failed file.")
                    oRequest.addSubRequest(registerRequestDict,'register')
                    modified = True
                  else:
                    gMonitor.addMark("Put successful",1)
                    gMonitor.addMark("File registration successful",1)
                    self.DataLog.addFileRecord(lfn,'Put',diracSE,'','TransferAgent')
                    self.DataLog.addFileRecord(lfn,'Register',diracSE,'','TransferAgent')
                    gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['put']))
                    gLogger.info("TransferAgent.execute: Successfully registered %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]['register']))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                    modified = True
                else:
                  gMonitor.addMark("Put failed",1)
                  self.DataLog.addFileRecord(lfn,'PutFail',diracSE,'','TransferAgent')
                  errStr = "TransferAgent.execute: Failed to put and register file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,diracSE,res['Value']['Failed'][lfn]))
              else:
                gMonitor.addMark("Put failed",1)
                self.DataLog.addFileRecord(lfn,'PutFail',diracSE,'','TransferAgent')
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
              gMonitor.addMark("Put",1)
              lfn = subRequestFile['LFN']
              file = subRequestFile['PFN']
              res = self.ReplicaManager.put(lfn, file, diracSE)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  gMonitor.addMark("Put successful",1)
                  self.DataLog.addFileRecord(lfn,'Put',diracSE,'','TransferAgent')
                  gLogger.info("TransferAgent.execute: Successfully put %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]))
                  oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                  modified = True
                else:
                  gMonitor.addMark("Put failed",1)
                  self.DataLog.addFileRecord(lfn,'PutFail',diracSE,'','TransferAgent')
                  errStr = "TransferAgent.execute: Failed to put file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,diracSE,res['Value']['Failed'][lfn]))
              else:
                gMonitor.addMark("Put failed",1)
                self.DataLog.addFileRecord(lfn,'PutFail',diracSE,'','TransferAgent')
                errStr = "TransferAgent.execute: Completely failed to put file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is a replicate and register operation
        elif operation == 'replicateAndRegister' or operation == 'replicateAndRegisterAndRemove':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          targetSE = subRequestAttributes['TargetSE']
          sourceSE = subRequestAttributes['SourceSE']
          if sourceSE == "None":
            sourceSE = ''
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
                    modified = True
                  else:
                    gMonitor.addMark("Replication successful",1)
                    gMonitor.addMark("Replica registration successful",1)
                    gLogger.info("TransferAgent.execute: Successfully replicated %s to %s in %s seconds." % (lfn,targetSE,res['Value']['Successful'][lfn]['replicate']))
                    gLogger.info("TransferAgent.execute: Successfully registered %s to %s in %s seconds." % (lfn,targetSE,res['Value']['Successful'][lfn]['register']))
                    oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                    modified = True
                else:
                  gMonitor.addMark("Replication failed",1)
                  errStr = "TransferAgent.execute: Failed to replicate and register file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,targetSE,res['Value']['Failed'][lfn]))
              else:
                gMonitor.addMark("Replication failed",1)
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
              gMonitor.addMark("Replicate",1)
              lfn = subRequestFile['LFN']
              res = self.ReplicaManager.replicate(lfn,targetSE,sourceSE=sourceSE)
              if res['OK']:
                if res['Value']['Successful'].has_key(lfn):
                  gMonitor.addMark("Replication successful",1)
                  gLogger.info("TransferAgent.execute: Successfully replicated %s to %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]))
                  oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                  modified = True
                else:
                  gMonitor.addMark("Replication failed",1)
                  errStr = "TransferAgent.execute: Failed to replicate file."
                  gLogger.error(errStr,"%s %s %s" % (lfn,targetSE,res['Value']['Failed'][lfn]))
              else:
                gMonitor.addMark("Replication failed",1)
                errStr = "TransferAgent.execute: Completely failed to replicate file."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("TransferAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is a get operation
        elif operation == 'get':
          gLogger.info("TransferAgent.execute: Attempting to execute %s sub-request." % operation)
          sourceSE = subRequestAttributes['TargetSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = str(subRequestFile['LFN'])
              pfn = str(subRequestFile['PFN'])
              got = False
              if sourceSE and pfn:
                res = self.ReplicaManager.getPhysicalFile(pfn,sourceSE)
                if res['Value']['Successful'].has_key(pfn):
                  got = True
              else:
                res = self.ReplicaManager.getFile(lfn)
                if res['Value']['Successful'].has_key(lfn):
                  got = False
              if got:
                gLogger.info("TransferAgent.execute: Successfully got %s." % lfn)
                oRequest.setSubRequestFileAttributeValue(ind,'transfer',lfn,'Status','Done')
                modified = True
              else:
                errStr = "TransferAgent.execute: Failed to get file."
                gLogger.error(errStr,lfn)
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

    if modified and jobID:
      result = self.finalizeRequest(requestName,jobID,sourceServer)
    return S_OK()
