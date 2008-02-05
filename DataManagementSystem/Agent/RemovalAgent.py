"""  RemovalAgent takes removal requests from the RequestDB and replicates them
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

AGENT_NAME = 'DataManagement/RemovalAgent'

class RemovalAgent(Agent):

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
    gMonitor.registerActivity("PhysicalRemovalAtt","Physical removal operations attempted/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("PhysicalRemovalDone","Successful physical removals/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("PhysicalRemovalFail","Failed physical removals/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("ReplicaRemovalAtt","Replcica removal operations attempted/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("ReplicaRemovalDone","Successful replica removals/min","TransferAgent", "Atemps", gMonitor.OP_SUM )
    gMonitor.registerActivity("ReplicaRemovalFail","Failed replica removals/min","TransferAgent", "Atemps", gMonitor.OP_SUM )

    self.maxNumberOfThreads = gConfig.getValue(self.section+'/NumberOfThreads',1)
    self.threadPoolDepth = gConfig.getValue(self.section+'/ThreadPoolDepth',1)
    self.threadPool = ThreadPool(1,self.maxNumberOfThreads)

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
      self.log.info("RemovalAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("RemovalAgent.execute: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("RemovalAgent.execute: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("RemovalAgent.execute: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("RemovalAgent.execute: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 60:
          obtainProxy = True

      if obtainProxy:
        self.log.info("RemovalAgent.execute: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("RemovalAgent.execute: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("RemovalAgent.execute: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("RemovalAgent.execute: Successfully renewed %s proxy." %self.proxyDN)

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
    requestString = res['Value']['requestString']
    requestName = res['Value']['requestName']
    sourceServer= res['Value']['Server']
    gLogger.info("RemovalAgent.execute: Obtained request %s" % requestName)
    oRequest = DataManagementRequest(request=requestString)

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
    for ind in range(res['Value']):
      gMonitor.addMark( "Execute", 1 )
      gLogger.info("RemovalAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'removal')['Value']
      if subRequestAttributes['Status'] == 'Waiting':
        subRequestFiles = oRequest.getSubRequestFiles(ind,'removal')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a put and register operation
        if operation == 'physicalRemoval':
          gLogger.info("RemovalAgent.execute: Attempting to execute %s sub-request." % operation)
          diracSE = subRequestAttributes['TargetSE']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              pfn = subRequestFile['PFN']
              lfn = subRequestFile['LFN']
              res = self.ReplicaManager.removePhysicalFile(diracSE,pfn)
              if res['OK']:
                if res['Value']['Successful'].has_key(pfn):
                  gLogger.info("RemovalAgent.execute: Successfully removed %s at %s in %s seconds." % (pfn,diracSE,res['Value']['Successful'][pfn]))
                  oRequest.setSubRequestFileAttributeValue(ind,'removal',lfn,'Status','Done')
                else:
                  errStr = "RemovalAgent.execute: Failed to remove physical file."
                  gLogger.error(errStr,"%s %s %s" % (pfn,diracSE,res['Value']['Failed'][pfn]))
              else:
                errStr = "RemovalAgent.execute: Completely failed to remove physical."
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

    return S_OK()
