# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Agent/RemovalAgent.py,v 1.18 2008/09/04 14:21:48 atsareg Exp $

"""  RemovalAgent takes removal requests from the RequestDB and replicates them
"""

__RCSID__ = "$Id: RemovalAgent.py,v 1.18 2008/09/04 14:21:48 atsareg Exp $"

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.RequestManagementSystem.Agent.RequestAgentMixIn import RequestAgentMixIn
import time,os,re
from types import *

AGENT_NAME = 'DataManagement/RemovalAgent'

class RemovalAgent(Agent,RequestAgentMixIn):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()

    gMonitor.registerActivity("Iteration",          "Agent Loops",                  "RemovalAgent",       "Loops/min",       gMonitor.OP_SUM)
    gMonitor.registerActivity("Execute",            "Request Processed",            "RemovalAgent",       "Requests/min",    gMonitor.OP_SUM)
    gMonitor.registerActivity("Done",               "Request Completed",            "RemovalAgent",       "Requests/min",    gMonitor.OP_SUM)

    gMonitor.registerActivity("PhysicalRemovalAtt", "Physical removals attempted",  "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("PhysicalRemovalDone","Successful physical removals", "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("PhysicalRemovalFail","Failed physical removals",     "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("PhysicalRemovalSize","Physically removed size",      "RemovalAgent",       "Bytes",           gMonitor.OP_ACUM)

    gMonitor.registerActivity("ReplicaRemovalAtt",  "Replica removal attempted",    "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("ReplicaRemovalDone", "Successful replica removals",  "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("ReplicaRemovalFail", "Failed replica removals",      "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)

    gMonitor.registerActivity("RemoveFileAtt",      "File removal attempted",       "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("RemoveFileDone",     "File removal done",            "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)
    gMonitor.registerActivity("RemoveFileFail",     "File removal failed",          "RemovalAgent",       "Removal/min",     gMonitor.OP_SUM)

    self.maxNumberOfThreads = gConfig.getValue(self.section+'/NumberOfThreads',0)
    self.threadPoolDepth = gConfig.getValue(self.section+'/ThreadPoolDepth',0)
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
    res = self.RequestDBClient.getRequest('removal')
    if not res['OK']:
      gLogger.info("RemovalAgent.execute: Failed to get request from database.")
      return S_OK()
    elif not res['Value']:
      gLogger.info("RemovalAgent.execute: No requests to be executed found.")
      return S_OK()
    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    sourceServer= res['Value']['Server']
    try:
      jobID = int(res['Value']['JobID'])
    except:
      jobID = 0
    gLogger.info("RemovalAgent.execute: Obtained request %s" % requestName)

    result = self.RequestDBClient.getCurrentExecutionOrder(requestName,sourceServer)
    if result['OK']:
      currentOrder = result['Value']
    else:
      return S_OK('Can not get the request execution order')

    oRequest = RequestContainer(request=requestString)

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('removal')
    if not res['OK']:
      errStr = "RemovalAgent.execute: Failed to obtain number of removal subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    gLogger.info("RemovalAgent.execute: Found %s sub requests." % res['Value'])

    ################################################
    # For all the sub-requests in the request
    modified = False
    for ind in range(res['Value']):
      gMonitor.addMark( "Execute", 1 )
      gLogger.info("RemovalAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'removal')['Value']
      subExecutionOrder = int(subRequestAttributes['ExecutionOrder'])
      subStatus = subRequestAttributes['Status']
      if subStatus == 'Waiting' and subExecutionOrder <= currentOrder:
        subRequestFiles = oRequest.getSubRequestFiles(ind,'removal')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a physical removal operation
        if operation == 'physicalRemoval':
          gLogger.info("RemovalAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          physicalFiles = []
          pfnToLfn = {}
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              pfn = str(subRequestFile['PFN'])
              lfn = str(subRequestFile['LFN'])
              pfnToLfn[pfn] = lfn
              physicalFiles.append(pfn)
          gMonitor.addMark('PhysicalRemovalAtt',len(physicalFiles))
          res = self.ReplicaManager.removePhysicalFile(diracSE,physicalFiles)
          if res['OK']:
            gMonitor.addMark('PhysicalRemovalDone',len(res['Value']['Successful'].keys()))
            for pfn in res['Value']['Successful'].keys():
              gLogger.info("RemovalAgent.execute: Successfully removed %s at %s in %s seconds." % (pfn,diracSE,res['Value']['Successful'][pfn]))
              oRequest.setSubRequestFileAttributeValue(ind,'removal',pfnToLfn[pfn],'Status','Done')
              modified = True
            gMonitor.addMark('PhysicalRemovalFail',len(res['Value']['Failed'].keys()))
            for pfn in res['Value']['Failed'].keys():
              gLogger.info("RemovalAgent.execute: Failed to remove physical file." , "%s %s %s" % (pfn,diracSE,res['Value']['Failed'][pfn]))
          else:
            gMonitor.addMark('PhysicalRemovalFail',len(physicalFiles))
            errStr = "RemovalAgent.execute: Completely failed to remove physical files."
            gLogger.error(errStr, res['Message'])

        ################################################
        #  If the sub-request is a physical removal operation
        elif operation == 'removeFile':
          gLogger.info("RemovalAgent.execute: Attempting to execute %s sub-request." % operation)
          lfns = []
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = str(subRequestFile['LFN'])
              lfns.append(lfn)
          gMonitor.addMark('RemoveFileAtt',len(lfns))
          res = self.ReplicaManager.removeFile(lfns)
          if res['OK']:
            gMonitor.addMark('RemoveFileDone',len(res['Value']['Successful'].keys()))
            for lfn in res['Value']['Successful'].keys():
              gLogger.info("RemovalAgent.execute: Successfully removed %s." % lfn)
              oRequest.setSubRequestFileAttributeValue(ind,'removal',lfn,'Status','Done')
              modified = True
            gMonitor.addMark('RemoveFileFail',len(res['Value']['Failed'].keys()))
            for lfn in res['Value']['Failed'].keys():
              if type(res['Value']['Failed'][lfn]) in StringTypes:
                if re.search('no such file or directory',res['Value']['Failed'][lfn].lower()):
                  gLogger.info("RemovalAgent.execute: File did not exist.",lfn)
                  oRequest.setSubRequestFileAttributeValue(ind,'removal',lfn,'Status','Done')
                else:
                  gLogger.info("RemovalAgent.execute: Failed to remove file.", "%s %s" % (lfn,res['Value']['Failed'][lfn]))
          else:
            gMonitor.addMark('RemoveFileFail',len(lfns))
            errStr = "RemovalAgent.execute: Completely failed to remove files files."
            gLogger.error(errStr, res['Message'])

        ################################################
        #  If the sub-request is a physical removal operation
        elif operation == 'replicaRemoval':
          gLogger.info("RemovalAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          lfns = []
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = str(subRequestFile['LFN'])
              lfns.append(lfn)
          gMonitor.addMark('ReplicaRemovalAtt',len(lfns))
          res = self.ReplicaManager.removeReplica(diracSE,lfns)
          if res['OK']:
            gMonitor.addMark('ReplicaRemovalDone',len(res['Value']['Successful'].keys()))
            for lfn in res['Value']['Successful'].keys():
              gLogger.info("RemovalAgent.execute: Successfully removed %s at %s in %s seconds." % (lfn,diracSE,res['Value']['Successful'][lfn]))
              oRequest.setSubRequestFileAttributeValue(ind,'removal',lfn,'Status','Done')
              modified = True
            gMonitor.addMark('PhysicalRemovalFail',len(res['Value']['Failed'].keys()))
            for lfn in res['Value']['Failed'].keys():
              gLogger.info("RemovalAgent.execute: Failed to remove replica." , "%s %s %s" % (lfn,diracSE,res['Value']['Failed'][lfn]))
          else:
            gMonitor.addMark('ReplicaRemovalFail',len(lfns))
            errStr = "RemovalAgent.execute: Completely failed to remove replicas."
            gLogger.error(errStr, res['Message'])

        ################################################
        #  If the sub-request is a request to the online system to retransfer
        elif operation == 'reTransfer':
          gLogger.info("RemovalAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              pfn = str(subRequestFile['PFN'])
              lfn = str(subRequestFile['LFN'])
              res = self.ReplicaManager.onlineRetransfer(diracSE,pfn)
              if res['OK']:
                if res['Value']['Successful'].has_key(pfn):
                  gLogger.info("RemovalAgent.execute: Successfully requested retransfer of %s." % pfn)
                  oRequest.setSubRequestFileAttributeValue(ind,'removal',lfn,'Status','Done')
                  modified = True
                else:
                  errStr = "RemovalAgent.execute: Failed to request retransfer."
                  gLogger.error(errStr,"%s %s %s" % (pfn,diracSE,res['Value']['Failed'][pfn]))
              else:
                errStr = "RemovalAgent.execute: Completely failed to request retransfer."
                gLogger.error(errStr, res['Message'])
            else:
              gLogger.info("RemovalAgent.execute: File already completed.")

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.error("RemovalAgent.execute: Operation not supported.", operation)

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty(ind,'removal')['Value']:
          oRequest.setSubRequestStatus(ind,'removal','Done')
          gMonitor.addMark( "Done", 1 )

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info("RemovalAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,sourceServer)

    if modified and jobID:
      result = self.finalizeRequest(requestName,jobID,sourceServer)

    return S_OK()
